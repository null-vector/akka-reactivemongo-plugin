package org.nullvector.crud

import akka.Done
import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.persistence.PersistentRepr
import akka.persistence.state.scaladsl.{DurableStateStore, DurableStateUpdateStore, GetObjectResult}
import org.nullvector.ReactiveMongoDriver
import org.nullvector.crud.ReactiveMongoCrud.Schema
import org.nullvector.typed.ReactiveMongoEventSerializer
import reactivemongo.api.bson.{BSONDateTime, BSONDocument}

import java.time.{Clock, Instant}
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
  }
}

class ReactiveMongoCrud(system: ActorSystem[?]) extends DurableStateStore[Any] with DurableStateUpdateStore[Any] {
  private implicit lazy val dispatcher: ExecutionContext =
    system.dispatchers.lookup(DispatcherSelector.fromConfig("akka-persistence-reactivemongo-dispatcher"))
  private val driver: ReactiveMongoDriver                = ReactiveMongoDriver(system)
  private val serializer: ReactiveMongoEventSerializer   = ReactiveMongoEventSerializer(system)
  private val utcClock: Clock                            = Clock.systemUTC()

  override def getObject(persistenceId: String): Future[GetObjectResult[Any]] = {
    for {
      coll              <- driver.crudCollection(persistenceId)
      (found, revision) <- coll.find(BSONDocument(Schema.persistenceId -> persistenceId)).one[BSONDocument].flatMap {
                             case Some(doc) =>
                               val manifest = doc.getAsOpt[String](Schema.manifest).get
                               val payload  = doc.getAsOpt[BSONDocument](Schema.payload).get
                               val revision = doc.getAsOpt[Long](Schema.revision).get
                               serializer
                                 .deserialize(PersistentRepr(payload = payload, manifest = manifest))
                                 .map(rep => Some(rep.payload) -> revision)
                             case None      => Future.successful(None, 0L)
                           }
    } yield GetObjectResult(found, revision)
  }
  override def upsertObject(persistenceId: String, revision: Long, value: Any, tag: String): Future[Done] = {
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
}
