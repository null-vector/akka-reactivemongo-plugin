package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.{EventAdapter, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class ReactiveMongoSnapshotSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val reactiveMongoSnapshotImpl: ReactiveMongoSnapshotImpl = new ReactiveMongoSnapshotImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new StateAdapter)

  "ReactiveMongoSnapshotImpl" should {
    "write and load" in {

      val pId = s"TestAggregate-${Random.nextLong().abs}"

      Await.ready(reactiveMongoSnapshotImpl.saveAsync(SnapshotMetadata(pId, 1), AggregateState("Miles", 23)), 7.seconds)
      Await.ready(reactiveMongoSnapshotImpl.saveAsync(SnapshotMetadata(pId, 2), AggregateState("Miles", 34)), 7.seconds)
      Await.ready(reactiveMongoSnapshotImpl.saveAsync(SnapshotMetadata(pId, 3), AggregateState("Miles", 56)), 7.seconds)

      val maybeSnapshot = Await.result(reactiveMongoSnapshotImpl.loadAsync(pId, SnapshotSelectionCriteria()), 7.seconds).get

      println(maybeSnapshot)

      maybeSnapshot.snapshot.asInstanceOf[AggregateState].age should be(56)
    }
  }

  override def afterAll {
    shutdown()
  }

  case class AggregateState(name: String, age: Int)

  class StateAdapter() extends EventAdapter[AggregateState] {
    override val manifest: String = "State1"

    override def tags(payload: Any): Set[String] = Set.empty

    private val handle = Macros.handler[AggregateState]

    override def payloadToBson(payload: AggregateState): BSONDocument = handle.write(payload)

    override def bsonToPayload(doc: BSONDocument): AggregateState = handle.read(doc)
  }

}