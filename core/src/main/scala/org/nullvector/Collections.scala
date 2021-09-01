package org.nullvector

import akka.Done
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{Actor, ActorLogging, ExtendedActorSystem}
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.commands.CommandException
import reactivemongo.api.indexes.Index.Aux
import reactivemongo.api.indexes.{CollectionIndexesManager, Index, IndexType}
import reactivemongo.api.{DB, FailoverStrategy, ReadPreference}

import scala.collection.mutable
import scala.concurrent._
import scala.util.{Failure, Success}

class Collections(system: ExtendedActorSystem) extends Actor with ActorLogging {

  import Collections._
  import system.dispatcher

  private val settings: ReactiveMongoPluginSettings  =
    ReactiveMongoPluginSettings(system.toTyped)
  private var currentDatabaseProvider                = settings.databaseProvider
  private val config: Config                         = system.settings.config
  private val journalPrefix                          =
    config.getString("akka-persistence-reactivemongo.prefix-collection-journal")
  private val snapshotPrefix                         = config.getString(
    "akka-persistence-reactivemongo.prefix-collection-snapshot"
  )
  private val verifiedNames: mutable.HashSet[String] = mutable.HashSet[String]()

  private val nameMapping: CollectionNameMapping = system.dynamicAccess
    .getClassFor[CollectionNameMapping](
      config.getString("akka-persistence-reactivemongo.collection-name-mapping")
    )
    .get
    .getDeclaredConstructor(classOf[Config])
    .newInstance(config)

  private def database: Future[DB] = {
    Future
      .successful(currentDatabaseProvider.database)
      .transform(_.flatMap(identity))
  }

  override def receive: Receive = {
    case SetDatabaseProvider(aDatabaseProvider, ack) =>
      currentDatabaseProvider = aDatabaseProvider
      ack.success(Done)

    case GetJournalCollectionNameFor(persistentId, promise) =>
      val name =
        s"$journalPrefix${nameMapping.collectionNameOf(persistentId).fold("")(name => s"_$name")}"
      promise completeWith verifiedJournalCollection(name)

    case GetSnapshotCollectionNameFor(persistentId, promise) =>
      val name =
        s"$snapshotPrefix${nameMapping.collectionNameOf(persistentId).fold("")(name => s"_$name")}"
      promise completeWith verifiedSnapshotCollection(name)

    case AddVerified(collectionName) => verifiedNames += collectionName

    case ShouldReindex(promisedDone) =>
      verifiedNames.clear()
      promisedDone success Done

    case GetJournals(response, collectionNames) =>
      val collections = database.flatMap(
        _.collectionNames
          .map { allNames =>
            val journalNames = allNames.filter(_.startsWith(journalPrefix))
            collectionNames match {
              case Nil => journalNames
              case _   =>
                journalNames.filter(name => collectionNames.exists(colName => name.endsWith(colName)))
            }
          }
          .flatMap { names =>
            Future.traverse(names) { name =>
              val promisedCollection = Promise[BSONCollection]
              promisedCollection completeWith verifiedJournalCollection(name)
              promisedCollection.future
            }
          }
      )
      response completeWith collections

    case CheckHealth(ack) =>
      val collections  = Promise[List[BSONCollection]]
      context.self ! GetJournals(collections, Nil)
      val eventualDone = collections.future.map(_.headOption).flatMap {
        case Some(collection) =>
          collection.find(BSONDocument.empty).one.map(_ => Done)
        case None             => Future.successful(Done)
      }
      ack completeWith eventualDone
  }

  private def verifiedJournalCollection(
      name: String
  ): Future[BSONCollection] = {
    database.flatMap { db =>
      val collection = db.collection[BSONCollection](name)
      if (!verifiedNames.contains(name)) {
        val eventualDone = for {
          _ <- createCollection(collection)
          _ <- ensureReplayEventsIndex(collection.indexesManager)
          _ <- ensureHighSeqNumberIndex(collection.indexesManager)
          _ <- ensureTagIndex(collection.indexesManager)
          _ <- Future.successful(self ! AddVerified(name))
        } yield ()
        eventualDone
          .onComplete {
            case Failure(exception) =>
              log.error(exception, exception.getMessage)
            case Success(_)         =>
          }
        eventualDone.map(_ => collection)
      } else Future.successful(collection)

    }

  }

  private def verifiedSnapshotCollection(
      name: String
  ): Future[BSONCollection] = {
    database.flatMap { db =>
      val collection = db.collection[BSONCollection](name)
      if (!verifiedNames.contains(name)) {
        val eventualDone = for {
          _ <- createCollection(collection)
          _ <- ensureLastSnapshotIndex(collection.indexesManager)
          _ <- ensureHighSeqNumberSnapshotIndex(collection.indexesManager)
          _ <- Future.successful(self ! AddVerified(name))
        } yield ()
        eventualDone
          .onComplete {
            case Failure(exception) =>
              log.error(exception, exception.getMessage)
            case Success(_)         =>
          }
        eventualDone.map(_ => collection)
      } else Future.successful(collection)
    }
  }

  private def createCollection(collection: BSONCollection) = {
    collection.create().recover { case CommandException.Code(48) => () }
  }

  private def ensureReplayEventsIndex(
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    val name = Some("replay_events")
    val key  = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.to_sn         -> IndexType.Ascending,
      Fields.from_sn       -> IndexType.Ascending
    )
    ensureIndex(index(key, name), indexesManager)
  }

  private def ensureHighSeqNumberIndex(
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    val name = Some("high_seq_number")
    val key  = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.to_sn         -> IndexType.Descending
    )
    ensureIndex(index(key, name), indexesManager)
  }

  private def ensureLastSnapshotIndex(
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    val key  = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.snapshot_ts   -> IndexType.Descending,
      Fields.sequence      -> IndexType.Descending
    )
    val name = Some("last_snapshot")
    ensureIndex(index(key, name, unique = true), indexesManager)
  }

  private def ensureHighSeqNumberSnapshotIndex(
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    val key  = Seq(
      Fields.persistenceId -> IndexType.Ascending,
      Fields.sequence      -> IndexType.Descending
    )
    val name = Some("high_seq_number")
    ensureIndex(index(key, name), indexesManager)
  }

  private def ensureTagIndex(
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    val tagsById = ensureIndex(
      index(
        Seq(
          "_id"       -> IndexType.Ascending,
          Fields.tags -> IndexType.Ascending
        ),
        Some("tags_by_id"),
        unique = true,
        sparse = true
      ),
      indexesManager
    )

    val allTags = ensureIndex(
      index(
        Seq(
          Fields.tags -> IndexType.Ascending
        ),
        Some("tags"),
        sparse = true
      ),
      indexesManager
    )

    tagsById flatMap (_ => allTags)
  }

  private def ensureIndex(
      index: Aux[BSONSerializationPack.type],
      indexesManager: CollectionIndexesManager
  ): Future[Unit] = {
    indexesManager
      .ensure(index.asInstanceOf[indexesManager.Index])
      .map(_ => ())
      .recover { case CommandException.Code(85) =>
        ()
      } // Index already exist with other name...
  }

  private def index(
      key: Seq[(String, IndexType)],
      name: Some[String],
      unique: Boolean = false,
      sparse: Boolean = false
  ): Aux[BSONSerializationPack.type] = {
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
      options = BSONDocument.empty
    )
  }
}

object Collections {

  sealed trait Command

  case class GetJournalCollectionNameFor(
      persistentId: String,
      response: Promise[BSONCollection]
  ) extends Command

  case class GetSnapshotCollectionNameFor(
      persistentId: String,
      response: Promise[BSONCollection]
  ) extends Command

  case class GetJournals(
      response: Promise[List[BSONCollection]],
      collectionNames: List[String]
  ) extends Command

  case class SetDatabaseProvider(
      databaseProvider: DatabaseProvider,
      ack: Promise[Done]
  ) extends Command

  case class ShouldReindex(ack: Promise[Done]) extends Command

  case class CheckHealth(ack: Promise[Done]) extends Command

  private case class AddVerified(collectionName: String) extends Command

}
