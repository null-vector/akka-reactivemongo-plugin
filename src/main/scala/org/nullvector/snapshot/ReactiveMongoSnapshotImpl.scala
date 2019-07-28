package org.nullvector.snapshot

import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.nullvector.{Fields, ReactiveMongoPlugin}
import reactivemongo.bson.BSONDocument

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
      maybeSelected <- maybeDoc.map(doc =>
        serializer.deserialize(doc.getAs[String](Fields.manifest).get, doc.getAs[BSONDocument](Fields.payload).get).map(event =>
          Some(SelectedSnapshot(
            SnapshotMetadata(
              persistenceId,
              doc.getAs[Long](Fields.sequence).get,
              doc.getAs[Long](Fields.snapshot_ts).get,
            ),
            event
          )
        ))
      ).getOrElse(Future.successful(None))
    } yield maybeSelected
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    for {
      collection <- rxDriver.snapshotCollection(metadata.persistenceId)
      (rep, _) <- serializer.serialize(PersistentRepr(snapshot))
      _ <- collection.insert(false).one(BSONDocument(
        Fields.persistenceId -> metadata.persistenceId,
        Fields.sequence -> metadata.sequenceNr,
        Fields.snapshot_ts -> metadata.timestamp,
        Fields.payload -> rep.payload.asInstanceOf[BSONDocument],
        Fields.manifest -> rep.manifest,
      ))
    } yield ()
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
