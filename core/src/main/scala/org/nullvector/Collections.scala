package org.nullvector

import akka.Done
import akka.actor.{Actor, ActorLogging, ExtendedActorSystem}
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.commands.CommandException
import reactivemongo.api.indexes.Index.Aux
import reactivemongo.api.indexes.{CollectionIndexesManager, Index, IndexType}

import scala.collection.mutable
import scala.concurrent._
import scala.util.{Failure, Success, Try}

class Collections(databaseProvider: DatabaseProvider, system: ExtendedActorSystem) extends Actor with ActorLogging {

  import Collections._
  import system.dispatcher

  private var currentDatabaseProvider = databaseProvider
  private val config: Config = system.settings.config
  private val journalPrefix = config.getString("akka-persistence-reactivemongo.prefix-collection-journal")
  private val snapshotPrefix = config.getString("akka-persistence-reactivemongo.prefix-collection-snapshot")
  private val verifiedNames: mutable.HashSet[String] = mutable.HashSet[String]()

  private val nameMapping: CollectionNameMapping = system.dynamicAccess.getClassFor[CollectionNameMapping](
    config.getString("akka-persistence-reactivemongo.collection-name-mapping")
  ).get.getDeclaredConstructor(classOf[Config]).newInstance(config)

  def database = currentDatabaseProvider.database

  override def receive: Receive = {
    case SetDatabaseProvider(databaseProvider, ack) =>
      currentDatabaseProvider = databaseProvider
      ack.success(Done)

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
          _ <- createCollection(collection)
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
          _ <- createCollection(collection)
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

  private def createCollection(collection: BSONCollection) = {
    collection.create().recover { case CommandException.Code(48) => () }
  }

  private def ensurePidSeqIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
    val name = Some("pid_seq")
    val key = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.to_sn -> IndexType.Descending
    )

    ensureIndex(index(key, name), indexesManager)
  }

  private def ensureSnapshotIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
    val key = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.sequence -> IndexType.Descending,
      Fields.snapshot_ts -> IndexType.Descending,
    )
    val name = Some("snapshot")
    ensureIndex(index(key, name, unique = true), indexesManager)
  }

  private def ensureTagIndex(indexesManager: CollectionIndexesManager): Future[Unit] = {
    ensureIndex(index(Seq(Fields.tags -> IndexType.Ascending), Some("tags"), sparse = true), indexesManager)
  }

  private def ensureIndex(index: Aux[BSONSerializationPack.type], indexesManager: CollectionIndexesManager): Future[Unit] = {
    indexesManager
      .ensure(index.asInstanceOf[indexesManager.Index])
      .map(_ => ())
      .recover { case CommandException.Code(85) => () } // Index already exist with other name...
  }

  private def index(key: Seq[(String, IndexType)], name: Some[String], unique: Boolean = false, sparse: Boolean = false): Aux[BSONSerializationPack.type] = {
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
}

object Collections {

  case class GetJournalCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetSnapshotCollectionNameFor(persistentId: String, response: Promise[BSONCollection])

  case class GetJournals(response: Promise[List[BSONCollection]])

  case class SetDatabaseProvider(databaseProvider: DatabaseProvider, ack: Promise[Done])

  private case class VerifyJournalIndices(collectionName: String)

  private case class VerifySnapshotIndices(collectionName: String)

  private case class AddVerified(collectionName: String)

}