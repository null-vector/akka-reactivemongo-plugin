package org.nullvector.util

import akka.actor.typed.Scheduler

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

object FutureExt {
  implicit class FutureExt[T](val future: Future[T]) extends AnyVal {
    /**
     * A non-blocking delayed execution.
     * @param duration
     * @param scheduler
     * @param ec
     * @return
     */
    def delayed(duration: FiniteDuration)(implicit scheduler: Scheduler, ec: ExecutionContext): Future[T] =
      future
        .flatMap { result =>
          val promise = Promise[T]()
          scheduler.scheduleOnce(duration, () => promise.success(result))
          promise.future
        }
  }

}
