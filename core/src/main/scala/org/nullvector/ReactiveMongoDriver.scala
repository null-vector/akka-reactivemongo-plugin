package org.nullvector

import akka.Done
import akka.actor.{ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.actor.typed.scaladsl.adapter._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.nullvector.ReactiveMongoDriver.QueryType.QueryType
import org.nullvector.ReactiveMongoDriver.{DatabaseProvider, QueryType}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json
import reactivemongo.api.DB
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object ReactiveMongoDriver extends ExtensionId[ReactiveMongoDriver] with ExtensionIdProvider {

  trait DatabaseProvider {
    def database: Try[DB]
  }

  override def lookup: ExtensionId[_ <: Extension] = ReactiveMongoDriver

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoDriver = new ReactiveMongoDriver(system)

  object QueryType extends Enumeration {
    type QueryType = Value
    val All, Recovery, HighestSeq, LoadSnapshot, EventsByTag = Value
  }

}

class ReactiveMongoDriver(system: ExtendedActorSystem) extends Extension {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private val dispatcherName = "akka-persistence-reactivemongo-dispatcher"
  private implicit val dispatcher: ExecutionContext = system.dispatchers.lookup(dispatcherName)
  private implicit val timeout: Timeout = Timeout(5.seconds)

  import Collections._

  private val collectionsProps: Props = Props(new Collections(system)).withDispatcher(dispatcherName)
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

  def shouldReindex(): Future[Done] = {
    val promisedDone = Promise[Done]()
    collections ! ShouldReindex(promisedDone)
    promisedDone.future
  }

  def serverStatus(): Future[BSONDocument] = {
    val promisedDocument = Promise[BSONDocument]()
    collections ! ServerStatus(promisedDocument)
    promisedDocument.future
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
    if (shouldExplain(queryType)) {
      queryBuilder.explain().cursor().collect[List]()
        .map(docs => Try(Json.parse(BsonTextNormalizer(docs.head))).foreach(println))
    }
  }

  def explainAgg(collection: BSONCollection)
                (queryType: QueryType.QueryType, stages: List[collection.PipelineOperator], hint: Option[collection.Hint]) = {
    if (shouldExplain(queryType)) {
      collection
        .aggregatorContext[BSONDocument](stages, explain = true, hint = hint)
        .prepared
        .cursor
        .collect[List]()
        .map(docs => Try(Json.parse(BsonTextNormalizer(docs.head))).foreach(println))
    }
  }


  private def shouldExplain(queryType: QueryType) = {
    explainOptions.exists(shouldType => shouldType == QueryType.All || shouldType == queryType)
  }
}