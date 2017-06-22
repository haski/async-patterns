import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import futures.FuturePatterns._
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import futures.JavaScheduler._

/**
  * Created by aronen on 19/05/2017.
  */
class FutureTests extends FlatSpec {

  "schedule" should "schedule execution in the future" in {

    val res = schedule(1 second) {
      System.currentTimeMillis()
    }

    val t1 = System.currentTimeMillis()
    val t2 = Await.result(res, 2 second)
    val time = t2 - t1
    println(s"time passed is $time")

    assert (time >= 1000)
    assert (time <= 1100)
  }

  "withTimeout" should "throw exception after timeout" in {

    val res = schedule(2 second)("hello") withTimeout (1 second)
    assertThrows[TimeoutException] {
      Await.result(res, 2 second)
    }
  }

  "withTimeout" should "do nothing if result arrives on time" in {

    val res = schedule(1 second)("hello") withTimeout (2 second)
    val finalRes = Await.result(res, 2 second)
    println(s"got $finalRes")
    assert (finalRes === "hello")
  }

  "delay" should "delay result" in {

    val res = Future(System.currentTimeMillis()) delay (1 second) map { t1 =>
      val t2 = System.currentTimeMillis()
      t2 - t1
    }

    val finalRes = Await.result(res, 2 second)
    println(s"got diff of $finalRes")
    assert (finalRes >= 1000)
    assert (finalRes <= 1100)
  }

  "retry(fixed)" should "be called 3 times" in {

    val res = retry(3, Fixed(1 second)) {
      case 0 => println("failing once")
        Future.failed(new RuntimeException("not good enough..."))

      case 1 => println("failing twice")
        Future.failed(new RuntimeException("still not good enough..."))

      case 2 => println("succeeding")
        Future.successful("great success !")
    }

    val finalResult = Await.result(res, 5 second)
    println(s"got $finalResult")
    assert (finalResult === "great success !")
  }

  "retry(conditional)" should "continue on TimeoutException" in {
    val policy = Fixed(100 milli, {
      case _: TimeoutException => true
      case _: Throwable => false
    })

    val res = retry(3, policy) {
      case 0 => println("fail on timeout, retrying")
        Future failed new TimeoutException("not responding...")
      case 1 => println("fail on timeout again, retrying")
        Future failed new TimeoutException("still not responding...")
      case 2 => println("fail on IO, stop retrying")
        Future failed new IOException("something bad happened")
    }

    try Await.result(res, 3 second) catch {
      case _: IOException => succeed
      case _: Throwable => fail("should get IOException")
    }

  }

  "doubleDispatch" should "return the short call" in {
    val switch = new AtomicBoolean()
    val res = doubleDispatch(1 second) {
      if (switch.compareAndSet(false, true)) {

        println("producing slow one...")
        Future ("slow response") delay (3 second)

      } else {

        println("producing fast one...")
        Future("fast response") delay (1 second)

      }
    }

    val (finalRes, time) = Await.result(res withTimer, 4 second)
    println(s"got $finalRes after ${time.toMillis}ms")
    assert (finalRes.get === "fast response")
  }

  "seq" should "collect all successful results" in {

    val f1 = Future("first")
    val f2 = Future("second")
    val f3 = Future failed new RuntimeException("failed result")
    val f4 = Future("third")

    val input = Map("1" -> f1, "2" -> f2, "3" -> f3, "4" -> f4)
    val res = map(input, ContinueOnError)

    val finalRes = Await.result(res, 1 second)
    println(s"got $finalRes")
    assert (finalRes.size === 3)
  }

  "seq" should "fail immediately if error occurs" in {

    val f1 = schedule(2 second)("first")
    val f2 = schedule(3 second)("second")
    val f3 = Future failed new RuntimeException("failed result")
    val f4 = schedule(4 second)("third")

    val input = Map("1" -> f1, "2" -> f2, "3" -> f3, "4" -> f4)
    val res = map(input, FailOnError)

    try {
      val finalRes = Await.result(res, 10 milli)
      fail("should throw exception.")
    } catch {
      case e: RuntimeException => assert (e.getMessage === "failed result")
      case _: Throwable => fail("wrong exception type")
    }

  }

  "seq" should "be faster than Future.sequence" in {
    val f1 = schedule(2 second)("value")
    val f2 = Future failed new RuntimeException("failed result")

    val res1 = Future.sequence(List(f1, f2)) withTimer
    val (_, time1) = Await.result(res1, 3 second)

    println(s"Future.sequence took: ${time1.toMillis}")

    val f3 = schedule(2 second)("value")
    val f4 = Future failed new RuntimeException("failed result")

    val res2 = seq(List(f3, f4)) withTimer
    val (_, time2) = Await.result(res2, 1 milli)

    println(s"FuturePatterns.seq took: ${time2.toMillis}")
  }


  "seq" should "stop on first error" in {

    val f1 = schedule(1 second)("first")
    val f3 = Future failed new RuntimeException("failed result") delay (2 second)
    val f2 = schedule(3 second)("second")
    val f4 = schedule(4 second)("third")

    val input = Map("1" -> f1, "2" -> f2, "3" -> f3, "4" -> f4)
    val res = map(input, StopOnError)

    val finalRes = Await.result(res, 5 second)
    println(s"result: $finalRes")
    assert (finalRes.size === 1)
  }


  "batch" should "divide large batch into small batches" in {
    val res = batch(1 to 6, 2, FailOnError) { num =>
      println(s"producing future $num")
      Future(s"result $num") delay (1 second)
    }

    val finalRes = Await.result(res, 4 second)
  }
}
