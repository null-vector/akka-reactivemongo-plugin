package org.nullvector.journal

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.{EventAdapter, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.BSONDocument

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class ReactiveMongoJournalSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = ConfigFactory.load()
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

      val eventualLong = reactiveMongoJournalImpl.asyncReadHighestSequenceNr(pId, 22l)

      Await.result(eventualLong, 7.second) should be(25l)
    }
  }

  override def afterAll {
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