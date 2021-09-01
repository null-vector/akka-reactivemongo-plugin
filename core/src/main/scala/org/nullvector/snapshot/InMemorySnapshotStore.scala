package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.nullvector.PersistInMemory
import org.nullvector.typed.ReactiveMongoEventSerializer
import reactivemongo.api.bson.BSONDocument

import scala.concurrent.Future

class InMemorySnapshotStore(val system: ActorSystem) extends SnapshotStoreOps {

  import PersistInMemory._
  import akka.actor.typed.scaladsl.adapter._
  import system.dispatcher

  private val eventSerializer: ReactiveMongoEventSerializer =
    ReactiveMongoEventSerializer(system.toTyped)
  private val persistInMemory: PersistInMemory              = PersistInMemory(system.toTyped)

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
    persistInMemory
      .snapshotsOf(persistenceId)
      .map(_.lastOption)
      .flatMap {
        case Some(entry) =>
          eventSerializer
            .deserialize(
              Seq(
                PersistentRepr(
                  entry.event,
                  entry.sequence,
                  persistenceId,
                  entry.manifest
                )
              )
            )
            .map(_.head)
            .map(snapshot =>
              Some(
                SelectedSnapshot(
                  SnapshotMetadata(
                    persistenceId,
                    entry.sequence,
                    entry.timestamp
                  ),
                  snapshot.payload
                )
              )
            )
        case None        =>
          Future.successful(None)
      }
  }

  override def saveAsync(
      meta: SnapshotMetadata,
      snapshot: Any
  ): Future[Unit] = {
    eventSerializer
      .serialize(Seq(PersistentRepr(snapshot)))
      .map(_.head)
      .map(dsrlzed =>
        SnapshotEntry(
          meta.sequenceNr,
          dsrlzed._1.manifest,
          dsrlzed._1.payload.asInstanceOf[BSONDocument],
          meta.timestamp
        )
      )
      .flatMap(entry => persistInMemory.addSnapshot(meta.persistenceId, entry))
      .map(_ => ())
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    persistInMemory
      .removeSnapshotOf(
        metadata.persistenceId,
        new SequenceRange(metadata.sequenceNr)
      )
      .map(_ => ())
  }

  override def deleteAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Unit] = {
    persistInMemory
      .removeSnapshotOf(
        persistenceId,
        new SequenceRange(criteria.minSequenceNr, criteria.maxSequenceNr)
      )
      .map(_ => ())
  }
}
