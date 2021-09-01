package org.nullvector.util

import java.util.{Timer, TimerTask}
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.FiniteDuration

object TimeoutPromise {
  private val timer: Timer = new Timer(true)

  def apply[T](timeout: FiniteDuration, onTimeoutMessage: () => String)(implicit
      ec: ExecutionContext
  ): Promise[T] = {
    val thePromise = Promise[T]
    val timerTask  = new TimerTask() {
      def run() = {
        if (!thePromise.isCompleted) {
          thePromise.tryFailure(PromiseTimeoutException(onTimeoutMessage()))
        }
      }
    }
    timer.schedule(timerTask, timeout.toMillis)
    thePromise.future.onComplete(_ => timerTask.cancel())
    thePromise
  }

  case class PromiseTimeoutException(message: String) extends Exception(message)

}
