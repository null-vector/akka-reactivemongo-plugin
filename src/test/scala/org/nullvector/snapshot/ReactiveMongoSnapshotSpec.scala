package org.nullvector.snapshot

import java.util.Date

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

class ReactiveMongoSnapshotSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  import system.dispatcher

  val snapshotter: ReactiveMongoSnapshotImpl = new ReactiveMongoSnapshotImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new StateAdapter)

  private val driver = ReactiveMongoDriver(system)

  dropCollOf("TestAggregate-x")

  "ReactiveMongoSnapshotImpl" should {
    "write and load" in {
      val pId = s"TestAggregate-read_write"

      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 111, new Date().getTime), AggregateState("Miles", 23)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 222, new Date().getTime), AggregateState("Miles", 34)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 56)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 78)), 7.seconds)

      val snapshot = Await.result(snapshotter.loadAsync(pId, SnapshotSelectionCriteria()), 7.seconds).get

      println(snapshot)

      snapshot.snapshot.asInstanceOf[AggregateState].age should be(78)
      snapshot.metadata.sequenceNr should be(333)
    }

    "delete snapshot" in {
      val pId = s"TestAggregate-delete"

      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 111, new Date().getTime), AggregateState("Miles", 23)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 222, new Date().getTime), AggregateState("Miles", 34)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 54)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 57)), 7.seconds)

      Await.result(snapshotter.deleteAsync(SnapshotMetadata(pId, 333)), 7.seconds)

      val snapshot = Await.result(snapshotter.loadAsync(pId, SnapshotSelectionCriteria()), 7.seconds).get

      println(snapshot)

      snapshot.snapshot.asInstanceOf[AggregateState].age should be(34)
      snapshot.metadata.sequenceNr should be(222)
    }

    "delete snapshot with criteria" in {
      val pId = s"TestAggregate-delete_criteria"

      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 111, new Date().getTime), AggregateState("Miles", 23)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 222, new Date().getTime), AggregateState("Miles", 34)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 54)), 7.seconds)
      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), AggregateState("Miles", 57)), 7.seconds)


      Await.result(snapshotter.deleteAsync(pId,
        SnapshotSelectionCriteria(maxSequenceNr = 333, minSequenceNr = 222)
      ), 7.seconds)

      val snapshot = Await.result(snapshotter.loadAsync(pId, SnapshotSelectionCriteria()), 7.seconds).get

      println(snapshot)

      snapshot.snapshot.asInstanceOf[AggregateState].age should be(23)
      snapshot.metadata.sequenceNr should be(111)
    }

  }

  override def afterAll {
    shutdown()
  }

  def dropCollOf(persistenceId: String): Unit = {
    val eventualBoolean = driver.snapshotCollection(persistenceId).flatMap(coll =>
      coll.drop(failIfNotFound = false)
    )

    Await.ready(eventualBoolean, 7.seconds)
  }

  case class AggregateState(name: String, age: Int)

  class StateAdapter() extends EventAdapter[AggregateState] {
    override val manifest: String = "State1"

    private val handle = Macros.handler[AggregateState]

    override def payloadToBson(payload: AggregateState): BSONDocument = handle.write(payload)

    override def bsonToPayload(doc: BSONDocument): AggregateState = handle.read(doc)
  }

}