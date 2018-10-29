package org.nullvector

import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.util.Timeout
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.CommandError
import reactivemongo.api.indexes.{CollectionIndexesManager, Index, IndexType}
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver =
    new ReactiveMongoDriver(system)
}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {

  protected implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-journal-dispatcher")

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
    val promise = Promise[BSONCollection]
    collections ! GetJournalCollectionNameFor(persistentId, promise)
    promise.future
  }

  def snapshotCollection(persistentId: String): Future[BSONCollection] = {
    val promise = Promise[BSONCollection]
    collections ! GetSnapshotCollectionNameFor(persistentId, promise)
    promise.future
  }

  def journals(): Future[List[BSONCollection]] = {
    val promise = Promise[List[BSONCollection]]
    collections ! GetJournals(promise)
    promise.future
  }


  class Collections() extends Actor {

    private val journalPrefix = "journal"
    private val snapshotPrefix = "snapshot"
    private val verifiedNames: mutable.HashSet[String] = mutable.HashSet[String]()

    private val nameMapping: CollectionNameMapping = system.getClass.getClassLoader.loadClass(
      system.settings.config.getString("akka-persistence-reactivemongo.collection-name-mapping")
    ).newInstance().asInstanceOf[CollectionNameMapping]

    override def receive: Receive = {
      case GetJournalCollectionNameFor(persistentId, promise) =>
        val name = s"$journalPrefix${nameMapping.collectionNameOf(persistentId).map(name => s"_$name").getOrElse("")}"
        promise complete Try(database.collection[BSONCollection](name))
        self ! VerifyJournalIndices(name)

      case GetSnapshotCollectionNameFor(persistentId, promise) =>
        val name = s"$snapshotPrefix${nameMapping.collectionNameOf(persistentId).map(name => s"_$name").getOrElse("")}"
        promise complete Try(database.collection[BSONCollection](name))
        self ! VerifySnapshotIndices(name)

      case VerifyJournalIndices(collectionName) =>
        if (!verifiedNames.contains(collectionName)) {
          val collection = database.collection[BSONCollection](collectionName)
          for {
            _ <- collection.create().recover { case e: CommandError if e.code.contains(48) => Unit }
            _ <- createPidSeqIndex(collection.indexesManager)
            _ <- createTagIndex(collection.indexesManager)
            _ <- Future.successful(self ! AddVerified(collectionName))
          } yield Unit
        }

      case VerifySnapshotIndices(collectionName) =>
        if (!verifiedNames.contains(collectionName)) {
          val collection = database.collection[BSONCollection](collectionName)
          for {
            _ <- collection.create().recover { case e: CommandError if e.code.contains(48) => Unit }
            _ <- createSnapshotIndex(collection.indexesManager)
            _ <- Future.successful(self ! AddVerified(collectionName))
          } yield Unit
        }

      case AddVerified(collectionName) => verifiedNames += collectionName

      case GetJournals(response) =>
        response completeWith (for {
          names <- database.collectionNames
          collections = names.filter(_.startsWith(journalPrefix)).map(database.collection[BSONCollection](_))
        } yield collections)
    }

    private def createPidSeqIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val indexName = "pid_seq"
      val index = Index(Seq(
        Fields.persistenceId -> IndexType.Ascending,
        Fields.sequence -> IndexType.Ascending
      ), Some(indexName), unique = true)
      indexesManager.create(index).map(_ => Unit)
    }

    private def createSnapshotIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val indexName = "snapshot"
      val index = Index(Seq(
        Fields.persistenceId -> IndexType.Ascending,
        Fields.sequence -> IndexType.Descending,
        Fields.timestamp -> IndexType.Descending,
      ), Some(indexName), unique = true)
      indexesManager.create(index).map(_ => Unit)
    }

    private def createTagIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val indexName = "tags"
      val index = Index(Seq(Fields.tags -> IndexType.Ascending), Some(indexName), sparse = true)
      indexesManager.create(index).map(_ => Unit)
    }
  }

  case class GetJournalCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetSnapshotCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetJournals(response: Promise[List[BSONCollection]])

  private case class VerifyJournalIndices(collectionName: String)

  private case class VerifySnapshotIndices(collectionName: String)

  private case class AddVerified(collectionName: String)

}