package org.nullvector.snapshot

import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.nullvector.{Fields, ReactiveMongoPlugin}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONValue}

import scala.concurrent.Future

trait ReactiveMongoSnapshotImpl extends ReactiveMongoPlugin {

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    for {
      collection <- rxDriver.snapshotCollection(persistenceId)
      maybeDoc <- collection.find(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
        Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
        Fields.snapshot_ts -> BSONDocument("$gte" -> criteria.minTimestamp),
        Fields.snapshot_ts -> BSONDocument("$lte" -> criteria.maxTimestamp),
      ), None)
        .one[BSONDocument]
      maybeSelected <- maybeDoc.map { doc =>
        (doc.getAs[BSONValue](Fields.snapshot_payload_skull) match {
          case Some(snapshot) => Future.successful(snapshot)
          case None => doc.getAs[String](Fields.manifest) match {
            case Some(manifest) =>
              serializer.deserialize(manifest, doc.getAs[BSONDocument](Fields.payload).get)
            case None =>
              Future.successful(doc.getAs[BSONValue](Fields.payload).get)
          }
        })
          .map(snapshot =>
            Some(SelectedSnapshot(
              SnapshotMetadata(
                persistenceId,
                doc.getAs[Long](Fields.sequence).get,
                doc.getAs[Long](Fields.snapshot_ts).get,
              ),
              snapshot
            )
            ))
      }.getOrElse(Future.successful(None))
    } yield maybeSelected
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { coll =>
      snapshot match {
        case payload: BSONValue =>
          insertDoc(coll, metadata, payload, None).map(_ => ())
        case _ =>
          for {
            (rep, _) <- serializer.serialize(PersistentRepr(snapshot))
            _ <- insertDoc(coll, metadata, rep.payload.asInstanceOf[BSONDocument], Option(rep.manifest))
          } yield ()
      }
    }

  }

  private def insertDoc(collection: BSONCollection, metadata: SnapshotMetadata, payload: BSONValue, maybeManifest: Option[String]): Future[Any] = {
    collection.insert(false).one(BSONDocument(
      Fields.persistenceId -> metadata.persistenceId,
      Fields.sequence -> metadata.sequenceNr,
      Fields.snapshot_ts -> metadata.timestamp,
      Fields.payload -> payload,
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
      ).flatMap(el => deleteBuilder.many(Seq(el)))
    }.map(_ => ())
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
      ).flatMap(el => deleteBuilder.many(Seq(el)))
    }.map(_ => ())
  }
}
