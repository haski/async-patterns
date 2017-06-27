package futures

import java.util.concurrent.TimeUnit

import com.ifesdjeen.timer.HashedWheelTimer

import scala.concurrent.duration.FiniteDuration

/**
  * Created by aronen on 22/06/2017.
  */
class HashedWheelScheduler extends Scheduler {
  private val _scheduler = new HashedWheelTimer()

  override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, task: => Unit): Cancellable =
    _scheduler.scheduleAtFixedRate(() => task, initialDelay.toNanos, interval.toNanos, TimeUnit.NANOSECONDS)

  override def scheduleOnce(initialDelay: FiniteDuration, task: => Unit): Cancellable =
    _scheduler.schedule(new Runnable {
      def run(): Unit = task
    }, initialDelay.toNanos, TimeUnit.NANOSECONDS)
}

object HashedWheelScheduler {
  implicit val scheduler = new HashedWheelScheduler
}
