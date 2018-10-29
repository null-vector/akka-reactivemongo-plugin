package org.nullvector.query

import akka.actor.ActorSystem
import akka.persistence.query.{NoOffset, PersistenceQuery}
import akka.persistence.{AtomicWrite, Persistence, PersistentRepr}
import akka.stream.{ActorMaterializer, NoMaterializer}
import akka.stream.scaladsl.Sink
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.nullvector.journal.ReactiveMongoJournalImpl
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

class ReactiveMongoReadJournalSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  import system.dispatcher

  protected lazy val rxDriver = ReactiveMongoDriver(system)


  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  private implicit val materializer = ActorMaterializer()
  val readJournal: ReactiveMongoScalaReadJournal =
    PersistenceQuery(system).readJournalFor[ReactiveMongoScalaReadJournal](ReactiveMongoJournalProvider.pluginId)

  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new SomeEventAdapter())

  "ReactiveMongoReadJournal" should {
    "Events by tag from NoOffset" in {
      val prefixReadColl = "ReadCollection"

      val dropForTest = rxDriver.journals()
        .map(_.filter(_.name.contains(prefixReadColl)).map(_.drop(failIfNotFound = false)))
        .flatMap(Future.sequence(_))
      Await.ready(dropForTest, 7.seconds)

      val eventualTriedUnitses = (1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"

        val events = (1 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        )
        reactiveMongoJournalImpl.asyncWriteMessages(events)
      }

      Await.result(Future.sequence(eventualTriedUnitses), 7.second)

      val eventualDone = readJournal.currentEventsByTag("event_tag_1", NoOffset).runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes = Await.result(eventualDone, 7.seconds)
      println(System.currentTimeMillis())

      envelopes.size shouldBe 500
    }

    "Events by tag from a given Offset" in {
      val prefixReadColl = "ReadCollection"

      val dropForTest = rxDriver.journals()
        .map(_.filter(_.name.contains(prefixReadColl)).map(_.drop(failIfNotFound = false)))
        .flatMap(Future.sequence(_))
      Await.ready(dropForTest, 7.seconds)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      Thread.sleep(2000)
      val offset = ObjectIdOffset(DateTime.now())

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      val eventualDone = readJournal.currentEventsByTag("event_tag_1", offset).runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes = Await.result(eventualDone, 7.seconds)
      println(System.currentTimeMillis())

      envelopes.size shouldBe 250

    }
  }

  override def afterAll {
    shutdown()
  }

  case class SomeEvent(name: String, price: Double)

  class SomeEventAdapter extends EventAdapter[SomeEvent] {

    private val r: BSONDocumentHandler[SomeEvent] = Macros.handler[SomeEvent]

    override val manifest: String = "some_event"

    override def tags(payload: Any): Set[String] = Set("event_tag_1", "event_tag_2")

    override def payloadToBson(payload: SomeEvent): BSONDocument = r.write(payload)

    override def bsonToPayload(doc: BSONDocument): SomeEvent = r.read(doc)
  }

}