package org.nullvector.journal

import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.util.Timeout
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.pattern._

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver =
    new ReactiveMongoDriver(system)
}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  private implicit val timeout: Timeout = Timeout(5.seconds)
  private val collectionNames: ActorRef = system.actorOf(Props(new CollectionNames()))

  private val database: DefaultDB = {
    val parsedURI = MongoConnection.parseURI("mongodb://localhost/test?rm.failover=900ms:21x1.30") match {
      case Success(_parsedURI) => _parsedURI
      case Failure(exception) => throw exception
    }
    val databaseName = parsedURI.db.getOrElse(throw new Exception("Missing database name"))
    Await.result(
      MongoDriver(system.settings.config).connection(parsedURI, strictUri = true).get.database(databaseName),
      30.seconds
    )
  }

  def journalCollection(persistentId: String): Future[BSONCollection] =
    (collectionNames ? GetJournalCollectionNameFor(persistentId)).mapTo[String].map(database.collection[BSONCollection](_))

  class CollectionNames() extends Actor {

    private val nameMapping: CollectionNameMapping = system.getClass.getClassLoader.loadClass(
      system.settings.config.getString("akka.akka-persistence-reactivemongo.collection-name-mapping")
    ).newInstance().asInstanceOf[CollectionNameMapping]

    override def receive: Receive = {
      case GetJournalCollectionNameFor(persistentId) =>
        sender() ! s"journal${nameMapping.collectionNameOf(persistentId).map(name => s"_$name").getOrElse("")}"
    }
  }

  case class GetJournalCollectionNameFor(persistentId: String)

}