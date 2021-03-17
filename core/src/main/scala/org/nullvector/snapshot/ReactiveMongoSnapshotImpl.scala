package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoDriver.QueryType
import org.nullvector.{Fields, ReactiveMongoPlugin}
import reactivemongo.api.bson._
import org.nullvector._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ReactiveMongoSnapshotImpl(val config: Config, val actorSystem: ActorSystem) extends ReactiveMongoPlugin with SnapshotStoreOps {

  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(actorSystem)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    for {
      collection <- rxDriver.snapshotCollection(persistenceId)
      queryBuilder = collection.find(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.snapshot_ts -> BSONDocument("$lte" -> criteria.maxTimestamp),
          Fields.snapshot_ts -> BSONDocument("$gte" -> criteria.minTimestamp),
          Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
          Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
        ), Option.empty[BSONDocument]
      )
      _ = rxDriver.explain(collection)(QueryType.LoadSnapshot, queryBuilder)
      maybeDoc <- queryBuilder.one[BSONDocument]
      maybeSelected <- maybeDoc.map { doc =>
        val payloadDoc = (doc.getAsOpt[BSONDocument](Fields.payload), doc.getAsOpt[BSONDocument](Fields.snapshot_payload)) match {
          case (Some(payloadDoc), _) => payloadDoc
          case (_, Some(payloadDoc)) => payloadDoc
          case _ => throw new Exception("No payload found")
        }
        (doc.getAsOpt[String](Fields.manifest) match {
          case Some(manifest) => serializer.deserialize(manifest, payloadDoc)
          case None => Future.successful(payloadDoc)
        }).map(snapshot =>
          Some(SelectedSnapshot(
            SnapshotMetadata(
              persistenceId,
              doc.getAsOpt[Long](Fields.sequence).get,
              doc.getAsOpt[Long](Fields.snapshot_ts).get,
            ),
            snapshot
          )))
      }.getOrElse(Future.successful(None))
    } yield maybeSelected
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { coll =>
      (snapshot match {
        case payload: BSONValue =>
          insertDoc(coll, metadata, payload, None)
        case _ =>
          for {
            (rep, _) <- serializer.serialize(PersistentRepr(snapshot))
            tryed <- insertDoc(coll, metadata, rep.payload.asInstanceOf[BSONDocument], Option(rep.manifest))
          } yield tryed
      }).transform(_.flatMap(identity))
    }

  }

  private def insertDoc(coll: collection.BSONCollection, metadata: SnapshotMetadata, payload: BSONValue, maybeManifest: Option[String]): Future[Try[Unit]] = {
    coll.insert(false).one(BSONDocument(
      Fields.persistenceId -> metadata.persistenceId,
      Fields.sequence -> metadata.sequenceNr,
      Fields.snapshot_ts -> metadata.timestamp,
      Fields.snapshot_payload -> payload,
      Fields.manifest -> maybeManifest,
    ))
      .map(result => if (result.writeErrors.isEmpty) Success(()) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n"))))
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> metadata.persistenceId,
          Fields.sequence -> metadata.sequenceNr,
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)).map(result => result.errmsg match {
        case Some(error) => throw new Exception(error)
        case None => ()
      }))
    }
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    rxDriver.snapshotCollection(persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
          Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)).map(result => result.errmsg match {
        case Some(error) => throw new Exception(error)
        case None => ()
      }))
    }
  }

}
