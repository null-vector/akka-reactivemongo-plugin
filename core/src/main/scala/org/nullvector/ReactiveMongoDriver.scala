package org.nullvector

import akka.Done
import akka.actor.{ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.nullvector.ReactiveMongoDriver.QueryType.QueryType
import org.nullvector.ReactiveMongoDriver.{DatabaseProvider, QueryType}
import play.api.libs.json.{JsString, Json}
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.{AsyncDriver, DB, MongoConnection}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Try

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  trait DatabaseProvider {
    def database: DB
  }

  override def lookup: ExtensionId[_ <: Extension] = ReactiveMongoDriver

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver = new ReactiveMongoDriver(system)

  object QueryType extends Enumeration {
    type QueryType = Value
    val All, Recovery, HighestSeq, LoadSnapshot, EventsByTag = Value
  }

}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {
  private val dispatcherName = "akka-persistence-reactivemongo-dispatcher"
  protected implicit val dispatcher: ExecutionContext = system.dispatchers.lookup(dispatcherName)
  private implicit val timeout: Timeout = Timeout(5.seconds)
  private val defaultProvider: DatabaseProvider = new DatabaseProvider {
    private lazy val db: DB = {
      val mongoUri = system.settings.config.getString("akka-persistence-reactivemongo.mongo-uri")
      Await.result(
        MongoConnection.fromString(mongoUri).flatMap { parsedUri =>
          val databaseName = parsedUri.db.getOrElse(throw new Exception("Missing database name"))
          AsyncDriver(system.settings.config).connect(parsedUri).flatMap(_.database(databaseName))
        },
        30.seconds
      )
    }

    override def database: DB = db
  }

  import Collections._

  private val collectionsProps: Props = Props(new Collections(defaultProvider, system)).withDispatcher(dispatcherName)
  private val collections: ActorRef = system.systemActorOf(collectionsProps, "ReactiveMongoDriverCollections")

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

  def withDatabaseProvider(databaseProvider: DatabaseProvider): Future[Done] = {
    val promisedDone = Promise[Done]()
    collections ! SetDatabaseProvider(databaseProvider, promisedDone)
    promisedDone.future
  }

  private lazy val explainOptions = {
    val config = ConfigFactory
      .systemEnvironment()
      .withFallback(ConfigFactory.systemProperties())

    def extractValue(conditionName: String) = {
      Try(config.getBoolean(conditionName)).toOption.filter(identity)
    }

    (extractValue("mongodb.explain-all").map(_ => QueryType.All) ::
        extractValue("mongodb.explain-recovery").map(_ => QueryType.Recovery) ::
        extractValue("mongodb.explain-highest-seq").map(_ => QueryType.HighestSeq) ::
        extractValue("mongodb.explain-load-snapshot").map(_ => QueryType.LoadSnapshot) ::
        extractValue("mongodb.explain-events-by-tag").map(_ => QueryType.EventsByTag) ::
          Nil).flatten
  }

  def explain(collection: BSONCollection)(queryType: QueryType.QueryType, queryBuilder: collection.QueryBuilder) = {
    if (shoudExplain(queryType)) {
      queryBuilder.explain().cursor().collect[List]()
        .map(docs => Try(Json.parse(BsonTextNormalizer(docs.head))).foreach(println))
    }
  }

  def explainAgg(collection: BSONCollection)
                (queryType: QueryType.QueryType, stages: (collection.AggregationFramework) => List[collection.PipelineOperator]) = {
    if (shoudExplain(queryType)) {
      collection
        .aggregatorContext[BSONDocument](stages(collection.AggregationFramework),explain = true,
          hint = Some(collection.hint(BSONDocument("_id" -> 1, Fields.tags -> 1))))
        .prepared
        .cursor
        .collect[List]()
        .map(docs => Try(Json.parse(BsonTextNormalizer(docs.head))).foreach(println))
    }
  }


  private def shoudExplain(queryType: QueryType) = {
    explainOptions.exists(shouldType => shouldType == QueryType.All || shouldType == queryType)
  }
}