package org.nullvector.snapshot

import java.util.Date

import akka.actor.ActorSystem
import akka.persistence.{SnapshotMetadata, SnapshotSelectionCriteria}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.{EventAdapter, Fields, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.api.bson.{BSONDocument, Macros}

import scala.concurrent.Await
import scala.concurrent.duration._

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


      snapshot.snapshot.asInstanceOf[AggregateState].age should be(78)
      snapshot.metadata.sequenceNr should be(333)
    }

    "load legacy snapshots" in {
      val pId = "TestAggregate-skully"

      val eventualInsert = ReactiveMongoDriver(system).snapshotCollection(pId).flatMap { col =>
        col.insert(false).one(BSONDocument(
          Fields.persistenceId -> pId,
          Fields.sequence -> 38L,
          Fields.snapshot_ts -> System.currentTimeMillis(),
          Fields.snapshot_payload -> BSONDocument("greeting" -> "Hello World"),
          Fields.manifest -> Option[String](null),
        ))
      }

      Await.result(eventualInsert, 2.seconds)

      val snapshot = Await.result(snapshotter.loadAsync(pId, SnapshotSelectionCriteria()), 2.seconds).get


      snapshot.snapshot.asInstanceOf[BSONDocument].getAsOpt[String]("greeting").get should be("Hello World")
      snapshot.metadata.sequenceNr should be(38)
    }

    "write and load Bson docs" in {
      val pId = s"TestAggregate-bson_doc"

      Await.ready(snapshotter.saveAsync(SnapshotMetadata(pId, 333, new Date().getTime), BSONDocument("greeting" -> "Hello World")), 7.seconds)

      val snapshot = Await.result(snapshotter.loadAsync(pId, SnapshotSelectionCriteria()), 7.seconds).get


      snapshot.snapshot.asInstanceOf[BSONDocument].getAsOpt[String]("greeting").get should be("Hello World")
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

    override def payloadToBson(payload: AggregateState): BSONDocument = handle.writeTry(payload).get

    override def bsonToPayload(doc: BSONDocument): AggregateState = handle.readDocument(doc).get
  }

}