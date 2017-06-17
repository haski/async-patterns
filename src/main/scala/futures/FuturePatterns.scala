package futures

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * Created by aronen, marenzon on 18/05/2017.
  */
object FuturePatterns {
  implicit val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  def schedule[T](duration: Duration)
                 (callable: => T)
                 (implicit scheduler: ScheduledExecutorService): Future[T] = {

    val res = Promise[T]()
    scheduler.schedule(() => {
      try
        res.success(callable)
      catch {
        case NonFatal(e) => res.failure(e)
      }
    }, duration.toMillis, TimeUnit.MILLISECONDS)

    res.future
  }

  def scheduleWith[T](duration: Duration)
                     (callable: => Future[T])
                     (implicit scheduler: ScheduledExecutorService): Future[T] = {

    val res = Promise[T]()
    scheduler.schedule(() => {
      res.completeWith(callable)
    }, duration.toMillis, TimeUnit.MILLISECONDS)

    res.future
  }

  implicit class FutureTimeout[T](future: Future[T]) {
    def withTimeout(duration: Duration)
                   (implicit scheduler: ScheduledExecutorService, executor: ExecutionContext): Future[T] = {
      val deadline = schedule(duration) {
        throw new TimeoutException("future timeout")
      }

      Future firstCompletedOf Seq(future, deadline)
    }
  }

  implicit class FutureDelay[T](future: Future[T]) {
    def delay(duration: Duration)
             (implicit scheduler: ScheduledExecutorService, executor: ExecutionContext): Future[T] = {
      future.flatMap(res => schedule(duration) {
        res
      })
    }
  }


  sealed trait StopCondition

  case object FailOnError extends StopCondition

  case object StopOnError extends StopCondition

  case object ContinueOnError extends StopCondition

  def map[K, T](futures: Map[K, Future[T]], stop: StopCondition = FailOnError)
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

  def seq[T](futures: Seq[Future[T]], stop: StopCondition = FailOnError)
            (implicit executor: ExecutionContext): Future[Seq[T]] = {
    map((1 to futures.size).zip(futures).toMap, stop).map(_.values.toList)
  }


  def doubleDispatch[T](duration: Duration)
                       (producer: => Future[T])
                       (implicit scheduler: ScheduledExecutorService, executor: ExecutionContext): Future[T] = {

    val done = new AtomicBoolean()
    val first = producer
    first.onComplete(_ => done.compareAndSet(false, true))

    val second = Promise[T]()
    scheduler.schedule(() => {
      if (done.compareAndSet(false, true)) {
        try {
          producer.onComplete(second.complete)
        } catch {
          case NonFatal(e) => second.failure(e)
        }
      }
    }, duration.toMillis, TimeUnit.MILLISECONDS)

    Future firstCompletedOf Seq(first, second.future)
  }

  sealed trait RetryPolicy

  case class Directly() extends RetryPolicy

  case class Pause(duration: Duration) extends RetryPolicy

  case class Exponential(duration: Duration) extends RetryPolicy

  case class Condition(predicate: Throwable => Boolean) extends RetryPolicy

  def retry[T](retries: Int, policy: RetryPolicy)
              (producer: Int => Future[T])
              (implicit scheduler: ScheduledExecutorService, executor: ExecutionContext): Future[T] = {

    def retry(retries: Int, policy: RetryPolicy, attempt: Int)
             (producer: Int => Future[T]): Future[T] = {
      val nextRetry = () => retry(retries, policy, attempt + 1)(producer)

      producer(attempt).recoverWith {
        case error: Throwable if attempt < retries - 1 =>
          policy match {
            case Condition(predicate) if predicate(error) => nextRetry()
            case Directly() => nextRetry()
            case Pause(duration) => scheduleWith(duration) {
              nextRetry()
            }
            case Exponential(duration) => scheduleWith(duration * (attempt + 1)) {
              nextRetry()
            }
          }
      }
    }

    retry(retries, policy, 0)(producer)
  }

  def batch[T, R](elements: Seq[T], batchSize: Int, stop: StopCondition = FailOnError)
                 (producer: T => Future[R])
                 (implicit executor: ExecutionContext): Future[Seq[R]] = {

    val stopFlag = new AtomicBoolean(false)

    def seqUnordered(elements: Seq[T], index: AtomicInteger, producer: T => Future[R],
                     stop: StopCondition): Future[List[R]] = {

      val currentIndex = index.getAndIncrement

      if (currentIndex >= elements.size) {
        Future(List[R]())
      } else {
        if (stopFlag.get()) {
          Future(List[R]())
        } else {
          producer(elements(currentIndex)).flatMap((result: R) => {
            seqUnordered(elements, index, producer, stop).map(results => result :: results)
          }).recoverWith { case error: Throwable =>
            stop match {
              case StopOnError => stopFlag.set(true)
                Future(List[R]())
              case FailOnError => stopFlag.set(true)
                Future.failed(error)
              case ContinueOnError => seqUnordered(elements, index, producer, stop)
            }
          }
        }
      }
    }

    val index = new AtomicInteger(0)
    val futures = List.fill(batchSize) {
      seqUnordered(elements, index, producer, stop)
    }

    Future.sequence(futures).map(_.flatten)
  }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._
    //
    //    val r = Await.result(batch(1 to 20, 2, ContinueOnError) { a =>
    //      if (a == 3) {
    //        Future.failed(new RuntimeException)
    //      } else {
    //        Future(a)
    //      }
    //    }, Duration.Inf)
    //
    //    println(r)


    val t = System.currentTimeMillis()
    val r = Await.result(retry(4, Condition(error => false)) { i =>
      println(i)
      //      if (i == 3) {
      //        Future(i)
      //      } else {
      Future.failed(new RuntimeException)
      //      }
    }, Duration.Inf)

    println(System.currentTimeMillis() - t)
  }
}