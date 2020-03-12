package org.nullvector.snapshot

import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.nullvector.{Fields, ReactiveMongoPlugin}
import reactivemongo.api.bson._
import org.nullvector._

import scala.concurrent.Future
import scala.util.Try

trait ReactiveMongoSnapshotImpl extends ReactiveMongoPlugin {

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    for {
      collection <- rxDriver.snapshotCollection(persistenceId)
      maybeDoc <- collection.find(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
          Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
          Fields.snapshot_ts -> BSONDocument("$gte" -> criteria.minTimestamp),
          Fields.snapshot_ts -> BSONDocument("$lte" -> criteria.maxTimestamp),
        ), Option.empty[BSONDocument]).one[BSONDocument]

      maybeSelected <- maybeDoc.map { doc =>
        val payloadDoc = (doc.getAsOpt[BSONDocument](Fields.payload), doc.getAsOpt[BSONDocument](Fields.snapshot_payload)) match {
          case (Some(payloaDoc), _) => payloaDoc
          case (_, Some(payloaDoc)) => payloaDoc
          case _ => throw new Exception("No payload found")
        }
        (doc.getAsOpt[String](Fields.manifest) match {
          case Some(manifest) => serializer.deserialize(manifest, payloadDoc)
          case None => Future.successful(payloadDoc)
        })
          .map(snapshot =>
            Some(SelectedSnapshot(
              SnapshotMetadata(
                persistenceId,
                doc.getAsOpt[Long](Fields.sequence).get,
                doc.getAsOpt[Long](Fields.snapshot_ts).get,
              ),
              snapshot
            )
            ))
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
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> metadata.persistenceId,
          Fields.sequence -> metadata.sequenceNr,
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)): Future[Try[Unit]])
    }.transform(_.flatMap(identity))
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
      ).flatMap(el => deleteBuilder.many(Seq(el)): Future[Try[Unit]])
    }.transform(_.flatMap(identity))
  }
}
