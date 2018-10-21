package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.typesafe.config.Config

import scala.concurrent.Future

class ReactiveMongoSnapshotStore(val config: Config) extends SnapshotStore with ReactiveMongoSnapshotImpl {

  lazy val actorSystem: ActorSystem = context.system

}
