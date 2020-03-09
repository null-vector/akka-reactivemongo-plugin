package org.nullvector.journal

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr, SnapshotMetadata}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.snapshot.ReactiveMongoSnapshotImpl
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.api.bson._

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random

class ReactiveMongoJournalSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  private val conf: Config = ConfigFactory.load()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = conf
    override val actorSystem: ActorSystem = system
  }

  private val reactiveMongoSnapshotImpl: ReactiveMongoSnapshotImpl = new ReactiveMongoSnapshotImpl {
    override val config: Config = conf
    override val actorSystem: ActorSystem = system
  }


  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new ListAdapter())
  serializer.addEventAdapter(new StringAdapter())
  serializer.addEventAdapter(new SomeAdapter())

  "ReactiveMongoJournalImpl" should {

    "asyncWriteMessages & asyncReadHighestSequenceNr" in {

      val pId = s"SomeCollection-${Random.nextLong().abs}"

      val list_1 = AList(4, 5)
      val list_2 = AList(1, 2, 3)
      val events = immutable.Seq(
        AtomicWrite(immutable.Seq(
          PersistentRepr(payload = list_1, persistenceId = pId, sequenceNr = 19),
          PersistentRepr(payload = "SimulatePersistAll", persistenceId = pId, sequenceNr = 20),
          PersistentRepr(payload = "SimulatePersistAll", persistenceId = pId, sequenceNr = 21),
          PersistentRepr(payload = "SimulatePersistAll", persistenceId = pId, sequenceNr = 22),
        )),
        AtomicWrite(PersistentRepr(payload = list_2, persistenceId = pId, sequenceNr = 23)),
        AtomicWrite(PersistentRepr(payload = Some(3.14), persistenceId = pId, sequenceNr = 24)),
        AtomicWrite(PersistentRepr(payload = "OneMoreEvent", persistenceId = pId, sequenceNr = 25))
      )

      val eventualTriedUnits = reactiveMongoJournalImpl.asyncWriteMessages(events)
      Await.result(eventualTriedUnits, 1.second)

      val eventualLong = reactiveMongoJournalImpl.asyncReadHighestSequenceNr(pId, 22)

      Await.result(eventualLong, 7.second) should be(25)
    }

    "write BsonDocs" in {

      val pId = s"SomeCollection-bson_${Random.nextLong().abs}"

      val events = immutable.Seq(
        AtomicWrite(PersistentRepr(BSONDocument("name" -> "John Coltrane"), persistenceId = pId, sequenceNr = 55)),
      )

      val eventualTriedUnits = reactiveMongoJournalImpl.asyncWriteMessages(events)
      Await.result(eventualTriedUnits, 1.second)

      val reps = ArrayBuffer[PersistentRepr]()
      Await.result(reactiveMongoJournalImpl.asyncReplayMessages(pId, 0, 1000, 10000)(rep => reps += rep), 7.seconds)

      reps.head.payload.asInstanceOf[BSONDocument].getAsOpt[String]("name").get shouldBe "John Coltrane"
      reps.head.sequenceNr shouldBe 55
    }

    "read max seqNr between Journal and Snapshot" in {
      val persistenceId = "MaxSeqNr-0"
      Await.result(ReactiveMongoDriver(system).journalCollection(persistenceId).map(_.drop(failIfNotFound = false)), 2.seconds)
      Await.result(ReactiveMongoDriver(system).snapshotCollection(persistenceId).map(_.drop(failIfNotFound = false)), 2.seconds)

      val events = immutable.Seq(
        AtomicWrite(PersistentRepr(payload = "AnEvent", persistenceId = persistenceId, sequenceNr = 26)),
        AtomicWrite(PersistentRepr(payload = "AnEvent", persistenceId = persistenceId, sequenceNr = 27))
      )

      Await.result(reactiveMongoJournalImpl.asyncWriteMessages(events), 1.second)
      Await.result(reactiveMongoSnapshotImpl.saveAsync(SnapshotMetadata(persistenceId, 35), BSONDocument("greeting" -> "Hello")), 2.seconds)
      Await.result(reactiveMongoSnapshotImpl.saveAsync(SnapshotMetadata(persistenceId, 36), BSONDocument("greeting" -> "Hello")), 2.seconds)

      val eventualHighest = reactiveMongoJournalImpl.asyncReadHighestSequenceNr(persistenceId, 0)

      Await.result(eventualHighest, 2.seconds) shouldBe 36
    }
  }

  override def afterAll: Unit = {
    shutdown()
  }

  case class AList(ints: Int*)

  class ListAdapter extends EventAdapter[AList] {

    override val manifest: String = "mi_lista_v1"

    override def tags(payload: Any): Set[String] = Set("list_tag")

    override def payloadToBson(payload: AList): BSONDocument = BSONDocument("ints" -> payload.ints)

    override def bsonToPayload(BSONDocument: BSONDocument): AList = ???

  }

  class SomeAdapter extends EventAdapter[Some[Double]] {

    override val manifest: String = "mi_some_v1"

    override def payloadToBson(payload: Some[Double]): BSONDocument = BSONDocument("some" -> payload)

    override def bsonToPayload(BSONDocument: BSONDocument): Some[Double] = ???

  }

  class StringAdapter extends EventAdapter[String] {

    override val manifest: String = "mi_string_v1"

    override def tags(payload: Any): Set[String] = Set("string_tag")

    override def payloadToBson(payload: String): BSONDocument = BSONDocument("string" -> payload)

    override def bsonToPayload(BSONDocument: BSONDocument): String = ???

  }

}