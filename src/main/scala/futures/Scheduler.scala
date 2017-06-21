package futures


import scala.concurrent.duration.FiniteDuration

/**
  * Created by aronen on 18/06/2017.
  */
trait Scheduler {
  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, task: => Unit): Unit
  def scheduleOnce(initialDelay: FiniteDuration, task: => Unit): Unit
}

