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

      val events = immutable.Seq(
        AtomicWrite(PersistentRepr(payload = List(1, 2, 3), persistenceId = pId, sequenceNr = 23)),
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

  class ListAdapter extends EventAdapter[List[Int]] {

    override val manifest: String = "mi_lista_v1"

    override def payloadToBson(payload: List[Int]): BSONDocument = BSONDocument("list" -> payload)

    override def bsonToPayload(BSONDocument: BSONDocument): List[Int] = ???

  }

  class SomeAdapter extends EventAdapter[Some[Double]] {

    override val manifest: String = "mi_some_v1"

    override def payloadToBson(payload: Some[Double]): BSONDocument = BSONDocument("some" -> payload)

    override def bsonToPayload(BSONDocument: BSONDocument): Some[Double] = ???

  }

  class StringAdapter extends EventAdapter[String] {

    override val manifest: String = "mi_string_v1"

    override def payloadToBson(payload: String): BSONDocument = BSONDocument("string" -> payload)

    override def bsonToPayload(BSONDocument: BSONDocument): String = ???

  }

}