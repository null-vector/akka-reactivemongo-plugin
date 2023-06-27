package org.nullvector.crud

import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.persistence.PersistentRepr
import akka.persistence.query.*
import akka.persistence.query.scaladsl.DurableStateStoreQuery
import akka.persistence.state.scaladsl.{DurableStateStore, DurableStateUpdateStore, GetObjectResult}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import org.nullvector.crud.ReactiveMongoCrud.{CrudOffset, Schema}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.nullvector.util.FutureExt.FutureExt
import org.nullvector.{ReactiveMongoDriver, ReactiveMongoPlugin}
import reactivemongo.akkastream.*
import reactivemongo.api.bson.{BSONDateTime, BSONDocument}

import java.time.{Clock, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object ReactiveMongoCrud {
  val pluginId = "akka-persistence-reactivemongo-crud"
  object Schema {
    val persistenceId = "pid"
    val payload       = "payload"
    val manifest      = "manifest"
    val revision      = "revision"
    val created       = "created"
    val updated       = "updated"
    val tags          = "tags"
    val deleted       = "deleted"
  }

  object CrudOffset {
    def latest(left: Offset, right: Offset): CrudOffset = from(left) -> from(right) match {
      case (left, right) if left >= right => left
      case (_, right)                     => right
    }

    def from(offset: Offset): CrudOffset = offset match {
      case NoOffset           => CrudOffset(Instant.EPOCH)
      case offset: CrudOffset => offset
      case other              => throw new IllegalArgumentException(s"$other is not assignable from CrudOffset")
    }
    def from(bsonDateTime: BSONDateTime) = new CrudOffset(Instant.ofEpochMilli(bsonDateTime.value))
  }

  case class CrudOffset(timestamp: Instant) extends Offset with Ordered[CrudOffset] {
    def asBson: BSONDateTime = BSONDateTime(timestamp.toEpochMilli)

    override def compare(that: CrudOffset): Int = timestamp.compareTo(that.timestamp)
  }
}

class ReactiveMongoCrud[State](system: ActorSystem[?]) extends DurableStateStore[State] with DurableStateUpdateStore[State] {
  private implicit lazy val dispatcher: ExecutionContext =
    system.dispatchers.lookup(DispatcherSelector.fromConfig(ReactiveMongoPlugin.pluginDispatcherName))
  private val driver: ReactiveMongoDriver                = ReactiveMongoDriver(system)
  private val serializer: ReactiveMongoEventSerializer   = ReactiveMongoEventSerializer(system)
  private val utcClock: Clock                            = Clock.systemUTC()

  override def getObject(persistenceId: String): Future[GetObjectResult[State]] = {
    for {
      coll              <- driver.crudCollection(persistenceId)
      (found, revision) <- coll.find(BSONDocument(Schema.persistenceId -> persistenceId)).one[BSONDocument].flatMap {
                             case Some(doc) =>
                               val revision = doc.getAsOpt[Long](Schema.revision).get
                               document2PersistentRepr(doc)
                                 .map(rep => Some(rep.payload.asInstanceOf[State]) -> revision)
                             case None      => Future.successful(None, 0L)
                           }
    } yield GetObjectResult(found, revision)
  }
  override def upsertObject(persistenceId: String, revision: Long, value: State, tag: String): Future[Done] = {
    val nowBsonDateTime = BSONDateTime(Instant.now(utcClock).toEpochMilli)
    for {
      coll <- driver.crudCollection(persistenceId)
      rep  <- serializer.serialize(PersistentRepr(value))
      _    <- coll
                .findAndUpdate(
                  BSONDocument(Schema.persistenceId -> persistenceId, Schema.revision -> (revision - 1)),
                  BSONDocument(
                    "$set"                          -> (BSONDocument(
                      Schema.payload  -> rep._1.payload.asInstanceOf[BSONDocument],
                      Schema.manifest -> rep._1.manifest,
                      Schema.revision -> revision,
                      Schema.tags     -> rep._2,
                      Schema.updated  -> nowBsonDateTime
                    ) ++ (if (revision == 1) BSONDocument(Schema.created -> nowBsonDateTime) else BSONDocument()))
                  ),
                  upsert = true
                )
    } yield Done
  }
  override def deleteObject(persistenceId: String): Future[Done] = {
    for {
      coll <- driver.crudCollection(persistenceId)
      _    <- coll.findAndRemove(BSONDocument(Schema.persistenceId -> persistenceId))
    } yield Done
  }

  override def deleteObject(persistenceId: String, revision: Long): Future[Done] = {
    for {
      coll <- driver.crudCollection(persistenceId)
      _    <- coll.findAndRemove(BSONDocument(Schema.persistenceId -> persistenceId, Schema.revision -> revision))
    } yield Done
  }

  def query(entityTypeHint: String, pullInterval: FiniteDuration): DurableStateStoreQuery[State] = new DurableStateStoreQuery[State] {
    private val amountOfCores: Int           = Runtime.getRuntime.availableProcessors()
    private implicit val mat: ActorSystem[?] = system

    override def currentChanges(tag: String, offset: Offset): Source[DurableStateChange[State], NotUsed] = {
      val maybeTag               = Some(tag).filterNot(_ == "")
      val eventualDocumentSource = driver
        .crudCollectionOfEntity(entityTypeHint)
        .map(collection =>
          collection
            .find(
              BSONDocument(
                Schema.updated -> BSONDocument("$gt" -> CrudOffset.from(offset).asBson)
              ) ++ maybeTag.fold(BSONDocument.empty)(_ => BSONDocument(Schema.tags -> tag))
            )
            .hint(
              collection.hint(maybeTag.fold(BSONDocument(Schema.updated -> 1))(_ => BSONDocument(Schema.updated -> 1, Schema.tags -> 1)))
            )
            .cursor[BSONDocument]()
            .documentSource()
        )

      Source
        .futureSource(eventualDocumentSource)
        .addAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
        .mapAsync(amountOfCores)(doc => document2PersistentRepr(doc).map(doc -> _))
        .map { case (doc, repr) =>
          val persistenceId = doc.getAsOpt[String](Schema.persistenceId).get
          val revision      = doc.getAsOpt[Long](Schema.revision).get
          val updatedTime   = doc.getAsOpt[BSONDateTime](Schema.updated).get
          doc.getAsOpt[Boolean](Schema.deleted) match {
            case Some(false) | None =>
              new UpdatedDurableState(
                persistenceId,
                revision,
                repr.payload.asInstanceOf[State],
                CrudOffset.from(updatedTime),
                updatedTime.value
              )
            case _                  =>
              new DeletedDurableState[State](persistenceId, revision, CrudOffset.from(updatedTime), updatedTime.value)
          }

        }
        .mapMaterializedValue(_ => NotUsed)
    }

    override def changes(tag: String, offset: Offset): Source[DurableStateChange[State], NotUsed] = {
      Source
        .unfoldAsync(offset)(offset =>
          Future
            .successful(())
            .delayed(pullInterval)(system.scheduler, implicitly[ExecutionContext])
            .flatMap(_ =>
              currentChanges(tag, offset)
                .runFold((offset, List[DurableStateChange[State]]())) { case ((lastOffset, acc), stateChange) =>
                  CrudOffset.latest(lastOffset, stateChange.offset) -> (stateChange :: acc)
                }
                .map(Some(_))
            )
        )
        .mapConcat(identity)
        .addAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
    }

    override def getObject(persistenceId: String): Future[GetObjectResult[State]] = ReactiveMongoCrud.this.getObject(persistenceId)
  }

  private def document2PersistentRepr(doc: BSONDocument): Future[PersistentRepr] = {
    val manifest = doc.getAsOpt[String](Schema.manifest).get
    val payload  = doc.getAsOpt[BSONDocument](Schema.payload).get
    serializer
      .deserialize(PersistentRepr(payload = payload, manifest = manifest))
  }
}
