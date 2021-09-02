package org.nullvector

import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.persistence.PersistentRepr
import akka.persistence.journal.{EventSeq, Tagged}
import akka.testkit.{ImplicitSender, TestKit}
import org.nullvector.EventSerializerSpec.{AnEvent, AnEventEventAdapter, OtherLegacyEvent, SomeLegacyEvent}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.{BSON, BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable._
import scala.concurrent.Await
import scala.concurrent.duration._

class EventSerializerSpec()
    extends TestKit(ActorSystem("ReactiveMongoPlugin"))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  private val serializer = ReactiveMongoEventSerializer(system.toTyped)
  serializer.addAdapters(Seq(new AnEventEventAdapter))
//  serializer.loadAkkaAdaptersFrom("custom.akka.persistent.adapters")

  "EventSerializer" should "serialize an Event" in {
    val eventualTuple = serializer
      .serialize(
        Seq(PersistentRepr(AnEvent("Hello World"), manifest = "AnEvent"))
      )
      .map(_.head)
    val document      = Await
      .result(eventualTuple, 1.second)
      ._1
      .payload
      .asInstanceOf[BSONDocument]

    document.getAsOpt[String]("string").get shouldBe ("Hello World")
  }

  it should "dserialize an Event" in {
    val eventualEvent = serializer
      .deserialize("AnEvent", BSONDocument("string" -> "Charlie"), "1", 1)
      .map(_.payload.asInstanceOf[AnEvent])
    val anEvent       = Await.result(eventualEvent, 1.second)

    anEvent.string shouldBe ("Charlie")
  }

  ignore should "serialize an AkkaEvent" in {
    val eventualTuple = serializer.serialize(
      Seq(
        PersistentRepr(
          SomeLegacyEvent("John", "Coltrane"),
          manifest = "SomeLegacyEvent"
        )
      )
    )
    val document      = Await
      .result(eventualTuple.map(_.head), 1.second)
      ._1
      .payload
      .asInstanceOf[BSONDocument]

    document.getAsOpt[String]("firstName").get shouldBe ("John")
    document.getAsOpt[String]("lastName").get shouldBe ("Coltrane")
  }

  ignore should "dserialize an AkkaEvent" in {
    val eventualEvent   = serializer
      .deserialize(
        "SomeLegacyEvent",
        BSONDocument("firstName" -> "Charlie", "lastName" -> "Parker"),
        "1",
        1
      )
      .map(_.payload.asInstanceOf[SomeLegacyEvent])
    val someLegacyEvent = Await.result(eventualEvent, 1.second)

    someLegacyEvent.firstName shouldBe ("Charlie")
    someLegacyEvent.lastName shouldBe ("Parker")
  }

  ignore should "dserialize other AkkaEvent" in {
    val eventualEvent   = serializer
      .deserialize(
        "OtherLegacyEvent",
        BSONDocument("firstName" -> "Charlie", "lastName" -> "Parker"),
        "1",
        1
      )
      .map(_.payload.asInstanceOf[OtherLegacyEvent])
    val someLegacyEvent = Await.result(eventualEvent, 1.second)

    someLegacyEvent.firstName shouldBe ("Charlie")
    someLegacyEvent.lastName shouldBe ("Parker")
  }

  ignore should "serialize other AkkaEvent" in {
    val eventualTuple = serializer.serialize(
      PersistentRepr(
        OtherLegacyEvent("John", "Coltrane"),
        manifest = "OtherLegacyEvent"
      )
    )
    val document      = Await
      .result(eventualTuple, 1.second)
      ._1
      .payload
      .asInstanceOf[BSONDocument]

    document.getAsOpt[String]("firstName").get shouldBe ("John")
    document.getAsOpt[String]("lastName").get shouldBe ("Coltrane")
  }

  it should "try to dserialize with non registered adapter" in {
    val eventualEvent = serializer
      .deserialize(
        "NonRegisteredEvent",
        BSONDocument("firstName" -> "Charlie", "lastName" -> "Parker"),
        "1",
        1
      )
      .map(_.asInstanceOf[OtherLegacyEvent])
    an[Exception] should be thrownBy Await.result(eventualEvent, 1.second)
  }

  ignore should "serialize other AkkaEvent with Tag" in {
    val eventualTuple = serializer.serialize(
      PersistentRepr(
        Tagged(OtherLegacyEvent("John", "Coltrane"), Set("tag")),
        manifest = "OtherLegacyEvent"
      )
    )
    val document      = Await
      .result(eventualTuple, 1.second)
      ._1
      .payload
      .asInstanceOf[BSONDocument]
    document.getAsOpt[String]("firstName").get shouldBe ("John")
    document.getAsOpt[String]("lastName").get shouldBe ("Coltrane")

  }

  it should "try serialize non registered event" in {
    val eventualTuple = serializer.serialize(
      PersistentRepr(BigDecimal("678"), manifest = "BogDecimalEvent")
    )
    an[Exception] should be thrownBy Await.result(eventualTuple, 1.second)
  }

  override def afterAll(): Unit = {
    shutdown()
  }

}

object EventSerializerSpec {

  case class AnEvent(string: String)

  case class SomeLegacyEvent(firstName: String, lastName: String)

  case class OtherLegacyEvent(firstName: String, lastName: String)

  class AnEventEventAdapter extends EventAdapter[AnEvent] {
    override val manifest: String = "AnEvent"

    override def tags(payload: AnEvent): Set[String] = Set("tag_1", "tag_2")

    private implicit val anEventMapper: BSONDocumentHandler[AnEvent] =
      Macros.handler[AnEvent]

    override def payloadToBson(payload: AnEvent): BSONDocument =
      BSON.writeDocument(payload).get

    override def bsonToPayload(doc: BSONDocument): AnEvent =
      BSON.readDocument(doc).get
  }

  class SomeAkkaEventAdapter(system: ExtendedActorSystem) extends akka.persistence.journal.EventAdapter {
    private val mapper: BSONDocumentHandler[SomeLegacyEvent] =
      Macros.handler[SomeLegacyEvent]

    override def fromJournal(event: Any, manifest: String): EventSeq = {
      val document = event.asInstanceOf[BSONDocument]
      EventSeq.single(mapper.readDocument(document).get)
    }

    override def manifest(event: Any): String = "SomeLegacyEvent"

    override def toJournal(event: Any): Any = {
      val legacyEvent = event.asInstanceOf[SomeLegacyEvent]
      mapper.writeTry(legacyEvent).get
    }
  }

  class OtherAkkaEventAdapter() extends akka.persistence.journal.EventAdapter {
    private implicit val mapper: BSONDocumentHandler[OtherLegacyEvent] =
      Macros.handler[OtherLegacyEvent]

    override def fromJournal(event: Any, manifest: String): EventSeq = {
      val document = event.asInstanceOf[BSONDocument]
      EventSeq.single(BSON.read(document).get)
    }

    override def manifest(event: Any): String = "OtherLegacyEvent"

    override def toJournal(event: Any): Any = {
      val legacyEvent = event.asInstanceOf[OtherLegacyEvent]
      mapper.writeTry(legacyEvent).get
    }
  }

}
