package org.nullvector

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.util.Timeout
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.commands.CommandException
import reactivemongo.api.indexes.{CollectionIndexesManager, Index, IndexType}
import reactivemongo.api.{AsyncDriver, DB, MongoConnection}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoDriver

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver =
    new ReactiveMongoDriver(system)
}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {

  protected implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-dispatcher")

  private implicit val timeout: Timeout = Timeout(5.seconds)
  private val collections: ActorRef = system.systemActorOf(Props(new Collections()), "ReactiveMongoDriverCollections")

  private val database: DB = {
    val mongoUri = system.settings.config.getString("akka-persistence-reactivemongo.mongo-uri")
    Await.result(
      MongoConnection.fromString(mongoUri).flatMap { parsedUri =>
        val databaseName = parsedUri.db.getOrElse(throw new Exception("Missing database name"))
        AsyncDriver(system.settings.config).connect(parsedUri).flatMap(_.database(databaseName))
      },
      30.seconds
    )
  }

  def journalCollection(persistentId: String): Future[BSONCollection] = {
    val promise = Promise[BSONCollection]()
    collections ! GetJournalCollectionNameFor(persistentId, promise)
    promise.future
  }

  def snapshotCollection(persistentId: String): Future[BSONCollection] = {
    val promise = Promise[BSONCollection]()
    collections ! GetSnapshotCollectionNameFor(persistentId, promise)
    promise.future
  }

  def journals(): Future[List[BSONCollection]] = {
    val promise = Promise[List[BSONCollection]]()
    collections ! GetJournals(promise)
    promise.future
  }


  class Collections() extends Actor with ActorLogging {
    private val journalPrefix = system.settings.config.getString("akka-persistence-reactivemongo.prefix-collection-journal")
    private val snapshotPrefix = system.settings.config.getString("akka-persistence-reactivemongo.prefix-collection-snapshot")
    private val verifiedNames: mutable.HashSet[String] = mutable.HashSet[String]()

    private val nameMapping: CollectionNameMapping = system.dynamicAccess.getClassFor[CollectionNameMapping](
      system.settings.config.getString("akka-persistence-reactivemongo.collection-name-mapping")
    ).get.newInstance()

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
          (for {
            _ <- collection.create().recover { case CommandException.Code(48) => () }
            _ <- ensurePidSeqIndex(collection.indexesManager)
            _ <- ensureTagIndex(collection.indexesManager)
            _ <- Future.successful(self ! AddVerified(collectionName))
          } yield ())
            .onComplete {
              case Failure(exception) => log.error(exception, exception.getMessage)
              case Success(_) =>
            }
        }

      case VerifySnapshotIndices(collectionName) =>
        if (!verifiedNames.contains(collectionName)) {
          val collection = database.collection[BSONCollection](collectionName)
          (for {
            _ <- collection.create().recover { case CommandException.Code(48) => () }
            _ <- ensureSnapshotIndex(collection.indexesManager)
            _ <- Future.successful(self ! AddVerified(collectionName))
          } yield ())
            .onComplete {
              case Failure(exception) => log.error(exception, exception.getMessage)
              case Success(_) =>
            }
        }

      case AddVerified(collectionName) => verifiedNames += collectionName

      case GetJournals(response) =>
        response completeWith (for {
          names <- database.collectionNames
          collections = names.filter(_.startsWith(journalPrefix)).map(database.collection[BSONCollection](_))
        } yield collections)
    }

    private def ensurePidSeqIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val name = Some("pid_seq")
      val key = Seq(
        Fields.persistenceId -> IndexType.Ascending,
        Fields.to_sn -> IndexType.Descending
      )

      indexesManager.ensure(index(key, name).asInstanceOf[indexesManager.Index]).map(_ => ())
    }

    private def ensureSnapshotIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      val key = Seq(
        Fields.persistenceId -> IndexType.Ascending,
        Fields.sequence -> IndexType.Descending,
        Fields.snapshot_ts -> IndexType.Descending,
      )
      val name = Some("snapshot")
      indexesManager.ensure(index(key, name, unique = true).asInstanceOf[indexesManager.Index]).map(_ => ())
    }

    private def ensureTagIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
      indexesManager.ensure(index(Seq(Fields.tags -> IndexType.Ascending), Some("tags"), sparse = true).asInstanceOf[indexesManager.Index]).map(_ => ())
    }
  }

  import scala.language.existentials

  private def index(key: Seq[(String, IndexType)], name: Some[String], unique: Boolean = false, sparse: Boolean = false) = {
    Index(BSONSerializationPack)(
      key = key,
      name = name,
      unique = unique,
      background = false,
      sparse = false,
      expireAfterSeconds = None,
      storageEngine = None,
      weights = None,
      defaultLanguage = None,
      languageOverride = None,
      textIndexVersion = None,
      sphereIndexVersion = None,
      bits = None,
      min = None,
      max = None,
      bucketSize = None,
      collation = None,
      wildcardProjection = None,
      version = None,
      partialFilter = None,
      options = BSONDocument.empty)
  }

  case class GetJournalCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetSnapshotCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetJournals(response: Promise[List[BSONCollection]])

  private case class VerifyJournalIndices(collectionName: String)

  private case class VerifySnapshotIndices(collectionName: String)

  private case class AddVerified(collectionName: String)

}