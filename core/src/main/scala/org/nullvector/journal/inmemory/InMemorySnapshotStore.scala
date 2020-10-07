package org.nullvector.journal.inmemory

import akka.actor.ActorSystem
import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.nullvector.ReactiveMongoEventSerializer
import org.nullvector.snapshot.SnapshotStoreOps

import scala.concurrent.Future

class InMemorySnapshotStore(val system: ActorSystem) extends SnapshotStoreOps {

  import akka.actor.typed.scaladsl.adapter._
  import PersistInMemory._

  private val eventSerializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(system)
  private val persistInMemory: PersistInMemory = PersistInMemory(system.toTyped)

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {

    eventSerializer.serialize(PersistentRepr(snapshot))
      //.map(deserialized => )
    ???
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {

    ???
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    ???
  }
}
