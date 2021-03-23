package org.nullvector

import akka.actor.{ActorSystem, ExtendedActorSystem}
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.api.{AsyncDriver, DB, MongoConnection}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try

class DefaultDatabaseProvider(system: ActorSystem) extends DatabaseProvider {

  import system.dispatcher

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val db = Try {
    val mongoUri = system.settings.config.getString("akka-persistence-reactivemongo.mongo-uri")
    logger.info("Connecting to {}", mongoUri)
    Await.result(
      MongoConnection.fromString(mongoUri).flatMap { parsedUri =>
        parsedUri.db match {
          case Some(databaseName) =>
            AsyncDriver(system.settings.config).connect(parsedUri).flatMap(_.database(databaseName))
          case None =>
            val exception = new IllegalStateException(s"Missing Database Name in $mongoUri")
            throw exception
        }
      },
      15.seconds
    )
  }

  override def database: Try[DB] = db

}
