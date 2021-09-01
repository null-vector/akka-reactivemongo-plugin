package org.nullvector.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}

import scala.concurrent.Future

trait SnapshotStoreOps {

  def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]]

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]

  def deleteAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Unit]

}
