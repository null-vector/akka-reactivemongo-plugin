package org.nullvector.journal

import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.util.Timeout
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.pattern._
import reactivemongo.api.commands.CommandError
import reactivemongo.api.commands.bson.DefaultBSONCommandError
import reactivemongo.api.indexes.{CollectionIndexesManager, Index, IndexType, IndexesManager}

import scala.collection.mutable

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver =
    new ReactiveMongoDriver(system)
}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  private implicit val timeout: Timeout = Timeout(5.seconds)
  private val collections: ActorRef = system.actorOf(Props(new Collections()))

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

  def journalCollection(persistentId: String): Future[BSONCollection] = {
    val promise = PromiseRef(system, 1.seconds)
    collections.tell(GetJournalCollectionNameFor(persistentId), promise.ref)
    promise.future.mapTo[BSONCollection]
  }

  class Collections() extends Actor {

    private val journalPrefix = "journal"
    private val verifiedNames: mutable.HashSet[String] = mutable.HashSet[String]()

    private val nameMapping: CollectionNameMapping = system.getClass.getClassLoader.loadClass(
      system.settings.config.getString("akka.akka-persistence-reactivemongo.collection-name-mapping")
    ).newInstance().asInstanceOf[CollectionNameMapping]

    override def receive: Receive = {
      case GetJournalCollectionNameFor(persistentId) =>
        val name = s"$journalPrefix${nameMapping.collectionNameOf(persistentId).map(name => s"_$name").getOrElse("")}"
        sender() ! database.collection[BSONCollection](name)
        self ! VerifyIndex(name)

      case VerifyIndex(collectionName) =>
        if (!verifiedNames.contains(collectionName)) {
          val collection = database.collection[BSONCollection](collectionName)
          for {
            _ <- collection.create().recover { case e: CommandError if e.code.contains(48) => Unit }
            _ <- createPidSeqIndex(collection.indexesManager)
            _ <- createTagIndex(collection.indexesManager)
            _ <- Future.successful(self ! AddVerified(collectionName))
          } yield Unit
        }

      case AddVerified(collectionName) =>
        println(s"$collectionName verified")
        verifiedNames += collectionName

    }

    private def createPidSeqIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val indexName = "pid_seq"
      val index = Index(Seq(Fields.persistenceId -> IndexType.Ascending, Fields.sequence -> IndexType.Ascending), Some(indexName), unique = true)
      indexesManager.create(index).map(_ => Unit)
    }

    private def createTagIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val indexName = "tags"
      val index = Index(Seq(Fields.tags -> IndexType.Ascending), Some(indexName), sparse = true)
      indexesManager.create(index).map(_ => Unit)
    }
  }

  case class GetJournalCollectionNameFor(persistentId: String)

  private case class VerifyIndex(collectionName: String)

  private case class AddVerified(collectionName: String)

}