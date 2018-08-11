package org.nullvector

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver =
    new ReactiveMongoDriver(system)
}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  private lazy val database: DefaultDB = {
    val parsedURI = MongoConnection.parseURI("mongodb://localhost/test") match {
      case Success(_parsedURI) => _parsedURI
      case Failure(exception) => throw exception
    }
    val databaseName = parsedURI.db.getOrElse(throw new Exception("Missing database name"))
    Await.result(MongoDriver(system.settings.config).connection(parsedURI).database(databaseName), 15.seconds)
  }

  def collectionFor(persistentId: String): Future[BSONCollection] = Future.successful(
    database.collection[BSONCollection](collectionNameFrom(persistentId))
  )

  private def collectionNameFrom(persistenceId: String): String = persistenceId.split("-")(0)

}