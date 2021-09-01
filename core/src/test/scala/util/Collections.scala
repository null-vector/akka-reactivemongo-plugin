package util

import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.nullvector.ReactiveMongoDriver

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Collections {

  val amountOfCores: Int = Runtime.getRuntime.availableProcessors()

  def dropAll(driver: ReactiveMongoDriver, prefix: Option[String] = None)(implicit
      actorSystem: ActorSystem[_]
  ) = {
    implicit val ec = actorSystem.executionContext
    Await.result(
      Source
        .future(driver.journals())
        .mapConcat(colls => prefix.fold(colls)(x => colls.filter(_.name == x)))
        .mapAsync(amountOfCores)(_.drop(failIfNotFound = false))
        .runWith(Sink.ignore)
        .flatMap(_ => driver.shouldReindex()),
      14.seconds
    )
  }

}
