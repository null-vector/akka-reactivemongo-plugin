package org.nullvector.queries

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.nullvector.journal.ReactiveMongoJournalImpl
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal, RefreshInterval}
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

class ReactiveMongoReadJournalSpec() extends TestKit(ActorSystem("ReactiveMongoReadJournal")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  import system.dispatcher

  protected lazy val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)
  private val amountOfCores: Int = Runtime.getRuntime.availableProcessors()

  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl(ConfigFactory.load(), system)

  private implicit val
  materializer: Materializer = Materializer.matFromSystem(system)
  val readJournal: ReactiveMongoScalaReadJournal = ReactiveMongoJournalProvider(system).scaladslReadJournal

  private val serializer = ReactiveMongoEventSerializer(system)
  serializer.addEventAdapter(new SomeEventAdapter())

  "ReactiveMongoReadJournal" should {

    "Events by tag from NoOffset" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.result(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 50).grouped(3).map(group =>
          AtomicWrite(group.map(jdx =>
            PersistentRepr(payload = SomeEvent(name(jdx), 23.45), persistenceId = pId, sequenceNr = jdx)
          ))
        ).toList)
      }.runWith(Sink.ignore), 14.seconds)

      {
        val eventualDone = readJournal.currentEventsByTag("event_tag_1", NoOffset).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 3.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 160
      }

      {
        val eventualDone = readJournal.currentEventsByTag("event_tag_other", NoOffset).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 3.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 170
      }

      {
        val eventualDone = readJournal.currentEventsByTags(Seq("event_tag_other", "event_tag_1"), NoOffset).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 3.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 330
      }
    }

    "Raw events by tag from NoOffset" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.result(Source(1 to 20).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 50).grouped(3).map(group =>
          AtomicWrite(group.map(jdx =>
            PersistentRepr(payload = SomeEvent(name(jdx), 23.45), persistenceId = pId, sequenceNr = jdx)
          ))
        ).toList)
      }.runWith(Sink.ignore), 14.second)

      val eventualDone = readJournal.currentRawEventsByTag("event_tag_1", NoOffset).runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes = Await.result(eventualDone, 3.seconds)
      println(System.currentTimeMillis())
      envelopes.size shouldBe 320
      envelopes.map(_.event).toList shouldBe an[List[BSONDocument]]
    }

    "current events by persistence id" in {
      val pId = "ReadCollection-currentByPersistenceId"

      dropAll()

      Await.result({
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 50).map(jdx =>
          AtomicWrite(PersistentRepr(payload = BSONDocument("test" -> "test"), persistenceId = pId, sequenceNr = jdx))
        ))
      }, 14.seconds)

      {
        val eventualDone = readJournal.currentEventsByPersistenceId(pId, 0, 10000).runWith(Sink.seq)
        println(System.currentTimeMillis())
        val envelopes = Await.result(eventualDone, 1.seconds)
        println(System.currentTimeMillis())
        envelopes.size shouldBe 50
        envelopes.head.event.isInstanceOf[BSONDocument] shouldBe true
        envelopes.head.event.asInstanceOf[BSONDocument].getAsOpt[String]("test").get shouldBe "test"
      }
    }


    "Events by tag from a given Offset" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(3000)
      val offset = ObjectIdOffset(DateTime.now())

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      val eventualDone = readJournal.currentEventsByTag("event_tag_1", offset).runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes = Await.result(eventualDone, 14.seconds)
      println(System.currentTimeMillis())

      envelopes.size shouldBe 250
    }

    "Infinite Events by tag" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      readJournal.eventsByTag("event_tag_1", NoOffset).async.runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Thread.sleep(1 * 1000)

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(2 * 1000)

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(3 * 1000)

      envelopes.size shouldBe 500
    }

    "Infinite Events by tag with Custom RefreshInterval" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      readJournal
        .eventsByTag("event_tag_1", NoOffset)
        .async
        .addAttributes(RefreshInterval(700.millis))
        .runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      readJournal
        .eventsByTag("some_tag", NoOffset)
        .async
        .addAttributes(RefreshInterval(700.millis))
        .runWith(Sink.foreach(println))

      Thread.sleep(1 * 1000)

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(1 * 1000)

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(1 * 1000)

      envelopes.size shouldBe 500
    }

    "Infinite Events by Id" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()

      val pId = s"$prefixReadColl-123"
      readJournal
        .eventsByPersistenceId(pId, 0L, Long.MaxValue)
        .async
        .runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Thread.sleep(1 * 1000)

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(immutable.Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(2 * 1000)

      Await.ready(Source(11 to 20).mapAsync(amountOfCores) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(immutable.Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(3 * 1000)

      envelopes.peek().persistenceId shouldBe pId
      envelopes.size shouldBe 20
    }

    "Infinite persistenceId" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      val ids = new ConcurrentLinkedQueue[String]()

      readJournal
        .persistenceIds()
        .async
        .runWith(Sink.foreach(e => ids.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { collId =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$collId", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(3 * 1000)

      ids.size shouldBe 250

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(3 * 1000)

      ids.size shouldBe 500
    }

    "current persistenceId" in {
      val prefixReadColl = "ReadCollection"

      dropAll()

      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { collId =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$collId", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }.runWith(Sink.ignore), 14.seconds)

      val ids = Await.result(readJournal.currentPersistenceIds().async.runWith(Sink.seq), 14.seconds)

      ids.size shouldBe 250

    }

  }

  private def dropAll() = {
    Await.ready(Source.future(rxDriver.journals())
      .mapConcat(identity)
      .mapAsync(amountOfCores)(_.drop(failIfNotFound = false))
      .runWith(Sink.ignore), 14.seconds)
  }

  override def afterAll: Unit = {
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

    override def tags(payload: SomeEvent): Set[String] = payload match {
      case SomeEvent(name, _) if name.startsWith("lechuga") => Set("event_tag_1", "event_tag_2")
      case SomeEvent(name, _) if name.startsWith("tomate") => Set("event_tag_other")
      case _ => Set.empty
    }

    override def payloadToBson(payload: SomeEvent): BSONDocument = r.writeTry(payload).get

    override def bsonToPayload(doc: BSONDocument): SomeEvent = r.readDocument(doc).get
  }

}