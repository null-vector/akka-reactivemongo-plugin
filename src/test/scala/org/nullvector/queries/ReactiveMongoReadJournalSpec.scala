package org.nullvector.queries

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.nullvector.journal.ReactiveMongoJournalImpl
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal, RefreshInterval}
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

class ReactiveMongoReadJournalSpec() extends TestKit(ActorSystem("ReactiveMongoReadJournal")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  import system.dispatcher

  protected lazy val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)


  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  val readJournal: ReactiveMongoScalaReadJournal = ReactiveMongoJournalProvider(system).scaladslReadJournal

  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new SomeEventAdapter())

  "ReactiveMongoReadJournal" should {
    "Events by tag from NoOffset" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.result(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 50).grouped(3).map(group =>
          AtomicWrite(group.map(jdx =>
            PersistentRepr(payload = SomeEvent(name(jdx), 23.45), persistenceId = pId, sequenceNr = jdx)
          ))
        ).toSeq)
      }), 7.second)

      {
        val eventualDone = readJournal.currentEventsByTag("event_tag_1", NoOffset).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 1.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 160
      }

      {
        val eventualDone = readJournal.currentEventsByTag("event_tag_other", NoOffset).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 1.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 170
      }

    }

    "Events by tag from a given Offset" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

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

    "Infinite Events by tag" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      readJournal.eventsByTag("event_tag_1", NoOffset).runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Thread.sleep(1 * 1000)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      Thread.sleep(2 * 1000)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      Thread.sleep(3 * 1000)

      envelopes.size shouldBe 500
    }

    "Infinite Events by tag with Custom RefreshInterval" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      readJournal
        .eventsByTag("event_tag_1", NoOffset)
        .addAttributes(RefreshInterval(700.millis))
        .runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      readJournal
        .eventsByTag("some_tag", NoOffset)
        .addAttributes(RefreshInterval(700.millis))
        .runWith(Sink.foreach(println))

      Thread.sleep(1 * 1000)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      Thread.sleep(1 * 1000)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }), 7.second)

      Thread.sleep(1 * 1000)

      envelopes.size shouldBe 500
    }

    "Infinite Events by Id" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      val pId = s"$prefixReadColl-123"
      readJournal.eventsByPersistenceId(pId, 0L, Long.MaxValue).runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Thread.sleep(1 * 1000)

      Await.ready(Future.sequence((1 to 10).map { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(immutable.Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }), 7.second)

      Thread.sleep(2 * 1000)

      Await.ready(Future.sequence((11 to 20).map { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(immutable.Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }), 7.second)

      Thread.sleep(3 * 1000)

      envelopes.peek().persistenceId shouldBe pId
      envelopes.size shouldBe 20
    }

    "Infinite persistenceId" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val ids = new ConcurrentLinkedQueue[String]()

      readJournal.persistenceIds().runWith(Sink.foreach(e => ids.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Await.ready(Future.sequence((1 to 10).map { collId =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$collId", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }), 7.second)

      Thread.sleep(3 * 1000)

      ids.size shouldBe 250

      Await.ready(Future.sequence((1 to 10).map { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }), 7.second)

      Thread.sleep(3 * 1000)

      ids.size shouldBe 500
    }

    "current persistenceId" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.ready(Future.sequence((1 to 10).map { collId =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$collId", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }), 7.second)

      val ids = Await.result(readJournal.currentPersistenceIds().runWith(Sink.seq), 7.seconds)

      ids.size shouldBe 250

    }

  }

  private def dropAll() = {
    val dropForTest = rxDriver.journals()
      .map(_.map(_.drop(failIfNotFound = false)))
      .flatMap(Future.sequence(_))
    Await.ready(dropForTest, 7.seconds)
  }

  override def afterAll {
    shutdown()
  }

  def name(idx: Int): String = idx % 3 match {
    case 0 => s"lechuga_$idx"
    case 1 => s"tomate_$idx"
    case 2 => s"cebolla_$idx"
  }

  case class SomeEvent(name: String, price: Double)

  class SomeEventAdapter extends EventAdapter[SomeEvent] {

    private val r: BSONDocumentHandler[SomeEvent] = Macros.handler[SomeEvent]

    override val manifest: String = "some_event"

    override def tags(payload: Any): Set[String] = payload match {
      case SomeEvent(name, _) if name.startsWith("lechuga") => Set("event_tag_1", "event_tag_2")
      case SomeEvent(name, _) if name.startsWith("tomate") => Set("event_tag_other")
      case _ => Set.empty
    }

    override def payloadToBson(payload: SomeEvent): BSONDocument = r.write(payload)

    override def bsonToPayload(doc: BSONDocument): SomeEvent = r.read(doc)
  }

}