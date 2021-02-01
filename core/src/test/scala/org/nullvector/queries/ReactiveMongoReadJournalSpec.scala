package org.nullvector.queries

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ActorSystem, typed}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.{DelayOverflowStrategy, Materializer}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{ImplicitSender, TestKit, TestKitBase}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.nullvector.journal.ReactiveMongoJournalImpl
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal, ReactiveMongoScalaReadJournalImpl, RefreshInterval}
import org.nullvector.{EventAdapter, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Random

class ReactiveMongoReadJournalSpec() extends TestKitBase with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {


  private lazy val config: Config = ConfigFactory
    .parseString("akka-persistence-reactivemongo.persistence-id-separator = \"\"")
    .withFallback(ConfigFactory.load())

  override lazy val system = typed.ActorSystem[Any](Behaviors.empty, "ReactiveMongoPlugin", config).classicSystem
  implicit lazy val dispatcher: ExecutionContextExecutor = system.dispatcher
  protected lazy val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)
  private val amountOfCores: Int = 10

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
      }.run(), 14.seconds)
      Thread.sleep(1500)
      val offset = ObjectIdOffset.newOffset()
      Thread.sleep(500)
      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.run(), 14.seconds)
      Thread.sleep(500)
      val eventualDone = readJournal.currentEventsByTag("event_tag_1", offset).runWith(Sink.seq)
      val envelopes = Await.result(eventualDone, 14.seconds)

      envelopes.size shouldBe 250
    }

    "Infinite Events by tag" in {
      val prefixReadColl = "ReadCollection"
      dropAll()
      val counter = new AtomicInteger()
      readJournal.eventsByTag("event_tag_1", NoOffset)
        .addAttributes(RefreshInterval(300.millis))
        .runWith(Sink.foreach(_ => counter.incrementAndGet())).recover {
        case e: Throwable => e.printStackTrace()
      }

      val eventualDone = Source(1 to 125)
        .delay(100.millis, DelayOverflowStrategy.backpressure)
        .mapAsync(amountOfCores) { idx =>
          val collId = (idx % 7) + 1
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages((1 to 4).map(jIdx =>
            AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
          ))
        }.run()
      Await.ready(eventualDone, 7.seconds)
      Thread.sleep(1000)
      counter.get() shouldBe 500
    }

    "Infinite Events by tag with Custom RefreshInterval" in {
      val prefixReadColl = "ReadCollection"
      dropAll()
      val counter = new AtomicInteger(0)
      readJournal
        .eventsByTag("event_tag_1", NoOffset)
        .addAttributes(RefreshInterval(5.millis))
        .runWith(Sink.foreach(e => counter.incrementAndGet())).recover {
        case e: Throwable => e.printStackTrace()
      }
      Thread.sleep(1000)
      Await.ready(Source(1 to 3).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)
      Thread.sleep(1000)
      Await.ready(Source(1 to 3).mapAsync(amountOfCores) { idx =>
        val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages((26 to 50).map(jIdx =>
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = jIdx))
        ))
      }.runWith(Sink.ignore), 14.seconds)

      Thread.sleep(1000)

      counter.get() shouldBe 150
    }

    "Infinite Events by Id" in {
      val prefixReadColl = "ReadCollection"
      dropAll()
      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()
      val pId = s"$prefixReadColl-123"
      readJournal
        .eventsByPersistenceId(pId, 0L, Long.MaxValue)
        .addAttributes(RefreshInterval(5.millis))
        .runWith(Sink.foreach(e => envelopes.add(e))).recover {
        case e: Throwable => e.printStackTrace()
      }
      Await.ready(Source(1 to 10).mapAsync(amountOfCores) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }.runWith(Sink.ignore), 14.seconds)
      Thread.sleep(700)
      Await.ready(Source(11 to 20).mapAsync(amountOfCores) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages(Seq(
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = idx))
        ))
      }.runWith(Sink.ignore), 14.seconds)
      Thread.sleep(500)
      envelopes.peek().persistenceId shouldBe pId
      envelopes.size shouldBe 20
    }

    "Infinite persistenceId" in {
      val prefixReadColl = "ReadCollection"
      dropAll()
      val ids = new AtomicInteger()
      readJournal
        .persistenceIds()
        .async
        .runWith(Sink.foreach(e => ids.incrementAndGet())).recover {
        case e: Throwable => e.printStackTrace()
      }
      Await.ready(Source(1 to 10).mapAsync(10) { collId =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$collId", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }.runWith(Sink.ignore), 14.seconds)
      Thread.sleep(2000)
      ids.get() shouldBe 250
      Await.ready(Source(1 to 10).mapAsync(10) { idx =>
        reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          AtomicWrite(PersistentRepr(payload = SomeEvent(s"lechuga_$idx", 23.45), persistenceId = pId, sequenceNr = 1))
        })
      }.runWith(Sink.ignore), 14.seconds)
      Thread.sleep(2000)
      ids.get() shouldBe 500
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
      val ids = Await.result(readJournal.currentPersistenceIds().runWith(Sink.seq), 14.seconds)
      ids.size shouldBe 250
    }
  }

  private def dropAll(prefix: Option[String] = None) = {
    Await.result(Source.future(rxDriver.journals())
      .mapConcat(colls => prefix.fold(colls)(x => colls.filter(_.name == x)))
      .mapAsync(amountOfCores)(_.drop(failIfNotFound = false))
      .runWith(Sink.ignore), 14.seconds)
  }

  override def afterAll(): Unit = {
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