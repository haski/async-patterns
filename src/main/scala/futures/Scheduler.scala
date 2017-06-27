package futures

import java.util.concurrent.ScheduledFuture

import scala.concurrent.duration.FiniteDuration

/**
  * Created by aronen on 18/06/2017.
  */
trait Scheduler {
  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, task: => Unit): Cancellable

  def scheduleOnce(initialDelay: FiniteDuration, task: => Unit): Cancellable

  sealed trait Cancellable {
    def cancel()

    def isCancelled: Boolean
  }

  implicit class DefaultCancellable(scheduledFuture: ScheduledFuture[_]) extends Cancellable {
    override def cancel(): Unit = scheduledFuture.cancel(false)

    override def isCancelled: Boolean = scheduledFuture.isCancelled
  }
}