package futures

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created by aronen, marenzon on 18/05/2017.
  */
object FuturePatterns {

  def schedule[T](duration: FiniteDuration)
                 (callable: => T)
                 (implicit scheduler: Scheduler): Future[T] = {

    val res = Promise[T]()
    scheduler.scheduleOnce(duration, {
      res.complete(Try(callable))
    })

    res.future
  }

  def scheduleWith[T](duration: FiniteDuration)
                     (callable: => Future[T])
                     (implicit scheduler: Scheduler): Future[T] = {

    schedule(duration)(callable).flatten
  }

  implicit class FutureTimeout[T](future: Future[T]) {
    def withTimeout(duration: FiniteDuration)
                   (implicit scheduler: Scheduler, executor: ExecutionContext): Future[T] = {
      val deadline = schedule(duration) {
        throw new TimeoutException("future timeout")
      }

      Future firstCompletedOf Seq(future, deadline)
    }
  }

  implicit class FutureDelay[T](future: Future[T]) {
    def delay(duration: FiniteDuration)
             (implicit scheduler: Scheduler, executor: ExecutionContext): Future[T] = {
      future.transformWith(res => scheduleWith(duration) {
        Future.fromTry(res)
      })
    }
  }

  implicit class FutureTimer[T](future: => Future[T]) {
    def withTimer(implicit executor: ExecutionContext): Future[(Try[T], FiniteDuration)] = {
      val t1 = System.nanoTime()
      future.transform(res => Success(res, FiniteDuration(System.nanoTime() - t1, TimeUnit.NANOSECONDS)))
    }
  }

  sealed trait StopCondition
  case object FailOnError extends StopCondition
  case object StopOnError extends StopCondition
  case object ContinueOnError extends StopCondition

  def sequence[T](futures: Seq[Future[T]], stop: StopCondition = FailOnError)
                 (implicit executor: ExecutionContext): Future[Seq[T]] = {
    collect((1 to futures.size).zip(futures).toMap, stop).map(_.values.toList)
  }

  def collect[K, T](futures: Map[K, Future[T]], stop: StopCondition = FailOnError)
                   (implicit executor: ExecutionContext): Future[Map[K, T]] = {
    val res = Promise[Map[K, T]]()

    import scala.collection.JavaConverters._
    val results = new ConcurrentHashMap[K, T]()
    val counter = new AtomicInteger(futures.size)

    futures.foreach { case (key, future) =>
      future.onComplete {
        case Success(result) => results.put(key, result)
          if (counter.decrementAndGet() == 0)
            res.trySuccess(results.asScala.toMap)

        case Failure(e) => stop match {
          case FailOnError => res.tryFailure(e)
          case StopOnError => res.trySuccess(results.asScala.toMap)
          case ContinueOnError => if (counter.decrementAndGet() == 0)
            res.trySuccess(results.asScala.toMap)
        }
      }
    }

    res.future
  }

  def doubleDispatch[T](duration: FiniteDuration)
                       (producer: => Future[T])
                       (implicit scheduler: Scheduler, executor: ExecutionContext): Future[T] = {

    val done = new AtomicBoolean()
    val first = producer
    first.onComplete(_ => done.compareAndSet(false, true))

    val second = Promise[T]()
    scheduler.scheduleOnce(duration, {
      if (done.compareAndSet(false, true)) {
        try {
          producer.onComplete(second.complete)
        } catch {
          case e: Throwable => second.failure(e)
        }
      }
    })

    Future firstCompletedOf Seq(first, second.future)
  }

  type Conditional = PartialFunction[Throwable, RetryPolicy]

  sealed trait RetryPolicy
  case object Immediate extends RetryPolicy
  case class Fixed(duration: FiniteDuration) extends RetryPolicy
  case class Exponential(duration: FiniteDuration) extends RetryPolicy

  def retry[T](retries: Int, policy: RetryPolicy)
              (producer: Int => Future[T])
              (implicit scheduler: Scheduler, executor: ExecutionContext): Future[T] = {
    retry(retries) { case _: Throwable => policy }(producer)
  }

  def retry[T](retries: Int)
              (policy: Conditional)
              (producer: Int => Future[T])
              (implicit scheduler: Scheduler, executor: ExecutionContext): Future[T] = {

    def retry(attempt: Int): Future[T] = {
      lazy val nextRetry = retry(attempt + 1)

      producer(attempt).recoverWith {
        case error: Throwable if (attempt < retries - 1) && policy.isDefinedAt(error) =>
          policy(error) match {
            case Immediate => nextRetry
            case Fixed(duration) => scheduleWith(duration)(nextRetry)
            case Exponential(duration) => scheduleWith(duration * (attempt + 1))(nextRetry)
          }
      }
    }

    retry(0)
  }

  def parallelFold[T, R, C](elements: Seq[T], parallelism: Int, stop: StopCondition = FailOnError)
                           (zero: C)
                           (op: (C, R) => C)
                           (producer: T => Future[R])
                           (implicit executor: ExecutionContext): Future[C] = {
    val futures: Future[Seq[R]] = parallelCollect(elements, parallelism, stop)(producer)
    futures.map(_.foldLeft(zero)(op))
  }

  def parallelCollect[T, R](elements: Seq[T], parallelism: Int, stop: StopCondition = FailOnError)
                           (producer: T => Future[R])
                           (implicit executor: ExecutionContext): Future[Seq[R]] = {

    def seqUnordered(stopFlag: AtomicBoolean, index: AtomicInteger): Future[List[R]] = {

      val currentIndex = index.getAndIncrement

      if (currentIndex >= elements.size) {
        Future(List[R]())
      } else {
        if (stopFlag.get()) {
          Future(List[R]())
        } else {
          producer(elements(currentIndex)).flatMap((result: R) => {
            seqUnordered(stopFlag, index).map(results => result :: results)
          }).recoverWith { case error: Throwable =>
            stop match {
              case StopOnError => stopFlag.set(true)
                Future(List[R]())
              case FailOnError => stopFlag.set(true)
                Future.failed(error)
              case ContinueOnError => seqUnordered(stopFlag, index)
            }
          }
        }
      }
    }

    val index = new AtomicInteger(0)
    val stopFlag = new AtomicBoolean(false)
    val futures = List.fill(parallelism) {
      seqUnordered(stopFlag, index)
    }

    sequence(futures).map(_.flatten)
  }
}