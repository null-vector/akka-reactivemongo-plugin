package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.typesafe.config.Config
import org.nullvector.{PersistInMemory, UnderlyingPersistenceFactory}

import scala.concurrent.Future

class ReactiveMongoSnapshotStore(val config: Config) extends SnapshotStore {

  private val snapshotOps: SnapshotStoreOps = UnderlyingPersistenceFactory(
    new ReactiveMongoSnapshotImpl(config, context.system),
    new InMemorySnapshotStore(context.system)
  )(context.system)

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] =
    snapshotOps.loadAsync(persistenceId, criteria)

  override def saveAsync(
      metadata: SnapshotMetadata,
      snapshot: Any
  ): Future[Unit] = snapshotOps.saveAsync(metadata, snapshot)

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    snapshotOps.deleteAsync(metadata)

  override def deleteAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Unit] = snapshotOps.deleteAsync(persistenceId, criteria)
}
