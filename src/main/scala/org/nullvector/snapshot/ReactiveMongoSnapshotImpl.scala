package org.nullvector.snapshot

import akka.persistence.snapshot.SnapshotStore
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
        Fields.timestamp -> BSONDocument("$gte" -> criteria.minTimestamp),
        Fields.timestamp -> BSONDocument("$lte" -> criteria.maxTimestamp),
      ), None)
        .one[BSONDocument]

      x = maybeDoc.map(doc =>
        serializer.deserialize(doc.getAs[String](Fields.manifest).get, doc.getAs[BSONDocument](Fields.event).get).map(event =>
          SelectedSnapshot(
            SnapshotMetadata(
              persistenceId,
              doc.getAs[Long](Fields.sequence).get,
              doc.getAs[Long](Fields.timestamp).get,
            ),
            event
          )
        )
      )

      maybeSelected <- x match {
        case Some(future) => future.map(Some(_))
        case _ => Future.successful(None)
      }

    } yield maybeSelected
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    for {
      collection <- rxDriver.snapshotCollection(metadata.persistenceId)
      (rep, _) <- serializer.serialize(PersistentRepr(snapshot))
      result <- collection.insert(BSONDocument(
        Fields.persistenceId -> metadata.persistenceId,
        Fields.sequence -> metadata.sequenceNr,
        Fields.timestamp -> metadata.timestamp,
        Fields.event -> rep.payload.asInstanceOf[BSONDocument],
        Fields.manifest -> rep.manifest,
      ))
    } yield result
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = ???

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = ???
}
