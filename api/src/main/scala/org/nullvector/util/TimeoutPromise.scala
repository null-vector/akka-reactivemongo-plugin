package org.nullvector.util

import java.util.{Timer, TimerTask}
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

object TimeoutPromise {
  private val timer: Timer = new Timer(true)

  def apply[T](timeout: FiniteDuration, onTimeoutMessage: => String): Promise[T] = {
    val thePromise = Promise[T]
    val timerTask = new TimerTask() {
      def run() = {
        if(!thePromise.isCompleted) thePromise.tryFailure(PromiseTimeoutException(onTimeoutMessage))
      }
    }
    timer.schedule(timerTask, timeout.toMillis)
    thePromise
  }

  case class PromiseTimeoutException(message: String) extends Exception(message)

}
