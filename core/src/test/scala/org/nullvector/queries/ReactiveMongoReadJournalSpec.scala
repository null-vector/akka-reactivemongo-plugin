package org.nullvector.queries

import akka.actor.typed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{DelayOverflowStrategy, Materializer}
import akka.testkit.{ImplicitSender, TestKitBase}
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.journal.ReactiveMongoJournalImpl
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal, RefreshInterval}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.nullvector.{EventAdapter, EventAdapterFactory, ReactiveMongoDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}
import util.Collections._

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random
import scala.util.matching.Regex

class ReactiveMongoReadJournalSpec()
    extends AnyFlatSpec
    with TestKitBase
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  lazy val config: Config                                = ConfigFactory
    .parseString("akka-persistence-reactivemongo.persistence-id-separator = \"\"")
    .withFallback(ConfigFactory.load())
  implicit lazy val typedSystem: typed.ActorSystem[Any]  = typed.ActorSystem[Any](Behaviors.empty, "ReactiveMongoPlugin", config)
  override implicit lazy val system                      = typedSystem.classicSystem
  implicit lazy val dispatcher: ExecutionContextExecutor = system.dispatcher
  lazy val rxDriver: ReactiveMongoDriver                 = ReactiveMongoDriver(system)
  val reactiveMongoJournalImpl: ReactiveMongoJournalImpl = new ReactiveMongoJournalImpl(ConfigFactory.load(), system)
  implicit val materializer: Materializer                = Materializer.matFromSystem(system)
  val readJournal: ReactiveMongoScalaReadJournal         = ReactiveMongoJournalProvider(system).readJournalFor(Nil)
  private val serializer                                 = ReactiveMongoEventSerializer(system.toTyped)
  serializer.addAdapters(Seq(new SomeEventAdapter(), EventAdapterFactory.adapt[SomeEventWithProperty]("SomeEventWithProperty", Set("TAG"))))

  override def beforeEach() = {
    dropAll(rxDriver)
    Await.result(rxDriver.journalCollection("j-0"), 1.second)
  }

  it should "Events by tag from NoOffset" in {
    val prefixReadColl = "ReadCollection_A"

    val tagNames = (1 to 3 map (_ => s"T${Random.nextInt().abs}")).toList

    Await.result(
      Source(1 to 10)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 50)
              .grouped(3)
              .map(group =>
                AtomicWrite(
                  group.map(jdx =>
                    PersistentRepr(
                      payload = SomeEvent(s"TAG:${tagNames(idx % 3)}", 23.45),
                      persistenceId = pId,
                      sequenceNr = jdx
                    )
                  )
                )
              )
              .toList
          )
        }
        .runWith(Sink.ignore),
      14.seconds
    )

    {
      val eventualDone =
        readJournal.currentEventsByTag(tagNames(1), NoOffset).runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes    = Await.result(eventualDone, 3.seconds)
      println(System.currentTimeMillis())
      envelopes.size shouldBe 200
    }

    {
      val eventualDone = readJournal
        .currentEventsByTag(tagNames.head, NoOffset)
        .runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes    = Await.result(eventualDone, 3.seconds)
      println(System.currentTimeMillis())
      envelopes.size shouldBe 150
    }

    {
      val eventualDone = readJournal
        .currentEventsByTags(Seq(tagNames(1), tagNames(2)), NoOffset)
        .runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes    = Await.result(eventualDone, 3.seconds)
      println(System.currentTimeMillis())
      envelopes.size shouldBe 350
    }
  }

  it should "Events by tag from NoOffset with Filter" in {
    val prefixReadColl = "ReadCollection_A_F"
    Await.result(
      Source(1 to 10)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_${UUID.randomUUID()}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 50)
              .grouped(3)
              .map(group =>
                AtomicWrite(
                  group
                    .map(jdx => PersistentRepr(payload = SomeEventWithProperty("TAG", jdx.toString), persistenceId = pId, sequenceNr = jdx))
                )
              )
              .toList
          )
        }
        .runWith(Sink.ignore),
      14.seconds
    )

    {
      val future = readJournal.currentEventsByTags(Seq("TAG"), NoOffset, BSONDocument("events.p.customerId" -> "5"), None).runWith(Sink.seq)
      Await.result(future, 1.second).size shouldBe 10
    }
  }

  "from infinite source" should "reads Events by tag from NoOffset with Filter" in {
    val prefixReadColl = "ReadCollection_F_Inf"
    val counter        = new AtomicInteger(0)
    readJournal
      .eventsByTags(Seq("TAG"), NoOffset, BSONDocument("events.p.customerId" -> "5"), None, 1.millis)
      .map(_ => (counter.incrementAndGet()))
      .run()

    Await.result(
      Source(1 to 10)
        .mapAsync(amountOfCores) { _ =>
          Thread.sleep(10)
          val pId = s"${prefixReadColl}_${UUID.randomUUID()}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 50)
              .grouped(3)
              .map(group =>
                AtomicWrite(
                  group
                    .map(jdx => PersistentRepr(payload = SomeEventWithProperty("TAG", jdx.toString), persistenceId = pId, sequenceNr = jdx))
                )
              )
              .toList
          )
        }
        .run(),
      14.seconds
    )

    Thread.sleep(50)
    counter.get() shouldBe 10
  }

  it should "Raw events by tag from NoOffset" in {
    val prefixReadColl = "ReadCollection_B"
    val tagNames       = (1 to 3 map (_ => s"T${Random.nextInt().abs}")).toList

    Await.result(
      Source(1 to 20)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 50)
              .grouped(3)
              .map(group =>
                AtomicWrite(
                  group.map(jdx =>
                    PersistentRepr(
                      payload = SomeEvent(s"TAG:${tagNames(idx % 3)}", 23.45),
                      persistenceId = pId,
                      sequenceNr = jdx
                    )
                  )
                )
              )
              .toList
          )
        }
        .runWith(Sink.ignore),
      14.second
    )

    val eventualDone =
      readJournal.currentRawEventsByTag(tagNames(1), NoOffset).runWith(Sink.seq)
    println(System.currentTimeMillis())
    val envelopes    = Await.result(eventualDone, 3.seconds)
    println(System.currentTimeMillis())
    envelopes.size shouldBe 350
    envelopes.map(_.event).toList shouldBe an[List[BSONDocument]]
  }

  it should "current events by persistence id" in {
    val pId = "ReadCollection_C-currentByPersistenceId"

    Await.result(
      {
        reactiveMongoJournalImpl.asyncWriteMessages(
          (1 to 50).map(jdx =>
            AtomicWrite(
              PersistentRepr(
                payload = BSONDocument("test" -> "test"),
                persistenceId = pId,
                sequenceNr = jdx
              )
            )
          )
        )
      },
      14.seconds
    )

    {
      val eventualDone = readJournal
        .currentEventsByPersistenceId(pId, 0, 10000)
        .runWith(Sink.seq)
      println(System.currentTimeMillis())
      val envelopes    = Await.result(eventualDone, 1.seconds)
      println(System.currentTimeMillis())
      envelopes.size shouldBe 50
      envelopes.head.event.isInstanceOf[BSONDocument] shouldBe true
      envelopes.head.event
        .asInstanceOf[BSONDocument]
        .getAsOpt[String]("test")
        .get shouldBe "test"
    }
  }

  it should "Events by tag from a given Offset" in {
    val prefixReadColl = "ReadCollection_D"

    Await.ready(
      Source(1 to 10)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 25).map(jIdx =>
              AtomicWrite(
                PersistentRepr(
                  payload = SomeEvent(s"lechuga_$idx", 23.45),
                  persistenceId = pId,
                  sequenceNr = jIdx
                )
              )
            )
          )
        }
        .run(),
      14.seconds
    )
    Thread.sleep(1000)
    val offset       = ObjectIdOffset.newOffset()
    Thread.sleep(1000)
    Await.ready(
      Source(1 to 10)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (26 to 50).map(jIdx =>
              AtomicWrite(
                PersistentRepr(
                  payload = SomeEvent(s"lechuga_$idx", 23.45),
                  persistenceId = pId,
                  sequenceNr = jIdx
                )
              )
            )
          )
        }
        .run(),
      14.seconds
    )
    Thread.sleep(1000)
    val eventualDone =
      readJournal.currentEventsByTag("event_tag_1", offset).runWith(Sink.seq)
    val envelopes    = Await.result(eventualDone, 14.seconds)

    envelopes.size shouldBe 250
  }

  it should "Infinite Events by tag" in {
    val prefixReadColl = "ReadCollection_E"

    val counter = new AtomicInteger()
    readJournal
      .eventsByTag("event_tag_1", NoOffset)
      .addAttributes(RefreshInterval(300.millis))
      .runWith(Sink.foreach(_ => counter.incrementAndGet()))
      .recover { case e: Throwable =>
        e.printStackTrace()
      }

    val eventualDone = Source(1 to 125)
      .delay(100.millis, DelayOverflowStrategy.backpressure)
      .mapAsync(amountOfCores) { idx =>
        val collId = (idx % 7) + 1
        val pId    = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
        reactiveMongoJournalImpl.asyncWriteMessages(
          (1 to 4).map(jIdx =>
            AtomicWrite(
              PersistentRepr(
                payload = SomeEvent(s"lechuga_$idx", 23.45),
                persistenceId = pId,
                sequenceNr = jIdx
              )
            )
          )
        )
      }
      .run()
    Await.ready(eventualDone, 7.seconds)
    Thread.sleep(1000)
    counter.get() shouldBe 500
  }

  it should "Infinite Events by tag with Custom RefreshInterval" in {
    val prefixReadColl = "ReadCollection_F"

    val counter = new AtomicInteger(0)
    readJournal
      .eventsByTag("event_tag_1", NoOffset)
      .addAttributes(RefreshInterval(5.millis))
      .runWith(Sink.foreach(e => counter.incrementAndGet()))
      .recover { case e: Throwable =>
        e.printStackTrace()
      }
    Thread.sleep(1000)
    Await.ready(
      Source(1 to 3)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (1 to 25).map(jIdx =>
              AtomicWrite(
                PersistentRepr(
                  payload = SomeEvent(s"lechuga_$idx", 23.45),
                  persistenceId = pId,
                  sequenceNr = jIdx
                )
              )
            )
          )
        }
        .runWith(Sink.ignore),
      14.seconds
    )
    Thread.sleep(1000)
    Await.ready(
      Source(1 to 3)
        .mapAsync(amountOfCores) { idx =>
          val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
          reactiveMongoJournalImpl.asyncWriteMessages(
            (26 to 50).map(jIdx =>
              AtomicWrite(
                PersistentRepr(
                  payload = SomeEvent(s"lechuga_$idx", 23.45),
                  persistenceId = pId,
                  sequenceNr = jIdx
                )
              )
            )
          )
        }
        .runWith(Sink.ignore),
      14.seconds
    )

    Thread.sleep(1000)

    counter.get() shouldBe 150
  }

  it should
    "Infinite Events by Id" in {
      val prefixReadColl = "ReadCollection_G"

      val envelopes = new ConcurrentLinkedQueue[EventEnvelope]()
      val pId       = s"$prefixReadColl-123"
      readJournal
        .eventsByPersistenceId(pId, 0L, Long.MaxValue)
        .addAttributes(RefreshInterval(1.millis))
        .runWith(Sink.foreach(e => envelopes.add(e)))

      Await.ready(
        Source(1 to 10)
          .mapAsync(amountOfCores) { idx =>
            reactiveMongoJournalImpl.asyncWriteMessages(
              Seq(
                AtomicWrite(
                  PersistentRepr(
                    payload = SomeEvent(s"lechuga_$idx", 23.45),
                    persistenceId = pId,
                    sequenceNr = idx
                  )
                )
              )
            )
          }
          .runWith(Sink.ignore),
        14.seconds
      )
      Await.ready(
        Source(11 to 20)
          .mapAsync(amountOfCores) { idx =>
            reactiveMongoJournalImpl.asyncWriteMessages(
              Seq(
                AtomicWrite(
                  PersistentRepr(
                    payload = SomeEvent(s"lechuga_$idx", 23.45),
                    persistenceId = pId,
                    sequenceNr = idx
                  )
                )
              )
            )
          }
          .runWith(Sink.ignore),
        14.seconds
      )
      Thread.sleep(100)
      envelopes.peek().persistenceId shouldBe pId
      envelopes.size shouldBe 20
    }

  it should "Infinite persistenceId" in {
    val prefixReadColl = "ReadCollection_H"

    val ids = new AtomicInteger()
    readJournal
      .persistenceIds()
      .async
      .runWith(Sink.foreach(e => ids.incrementAndGet()))
      .recover { case e: Throwable =>
        e.printStackTrace()
      }
    Await.ready(
      Source(1 to 10)
        .mapAsync(10) { collId =>
          reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
            val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
            AtomicWrite(
              PersistentRepr(
                payload = SomeEvent(s"lechuga_$collId", 23.45),
                persistenceId = pId,
                sequenceNr = 1
              )
            )
          })
        }
        .runWith(Sink.ignore),
      14.seconds
    )
    Thread.sleep(2000)
    ids.get() shouldBe 250
    Await.ready(
      Source(1 to 10)
        .mapAsync(10) { idx =>
          reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
            val pId = s"${prefixReadColl}_$idx-${Random.nextLong().abs}"
            AtomicWrite(
              PersistentRepr(
                payload = SomeEvent(s"lechuga_$idx", 23.45),
                persistenceId = pId,
                sequenceNr = 1
              )
            )
          })
        }
        .runWith(Sink.ignore),
      14.seconds
    )
    Thread.sleep(2000)
    ids.get() shouldBe 500
  }

  it should "current persistenceId" in {
    val prefixReadColl = "ReadCollection_I"
    Await.ready(
      Source(1 to 10)
        .mapAsync(amountOfCores) { collId =>
          reactiveMongoJournalImpl.asyncWriteMessages((1 to 25).map { jIdx =>
            val pId = s"${prefixReadColl}_$collId-${Random.nextLong().abs}"
            AtomicWrite(
              PersistentRepr(
                payload = SomeEvent(s"lechuga_$collId", 23.45),
                persistenceId = pId,
                sequenceNr = 1
              )
            )
          })
        }
        .runWith(Sink.ignore),
      14.seconds
    )
    val ids            = Await.result(
      readJournal.currentPersistenceIds().runWith(Sink.seq),
      14.seconds
    )
    ids.size shouldBe 250
  }

  override def afterAll(): Unit = {
    shutdown()
  }

  case class SomeEvent(name: String, price: Double)
  case class SomeEventWithProperty(name: String, customerId: String)

  class SomeEventAdapter extends EventAdapter[SomeEvent] {

    private val r: BSONDocumentHandler[SomeEvent] = Macros.handler[SomeEvent]

    override val manifest: String = "some_event"

    private val tagName: Regex = """TAG:(\w+)""".r

    override def tags(payload: SomeEvent): Set[String] = payload match {
      case SomeEvent(name, _) if name.startsWith("lechuga") =>
        Set("event_tag_1", "event_tag_2")
      case SomeEvent(name, _) if name.startsWith("tomate")  =>
        Set("event_tag_other")
      case SomeEvent(tagName(name), _)                      => Set(name)
      case _                                                => Set.empty
    }

    override def payloadToBson(payload: SomeEvent): BSONDocument =
      r.writeTry(payload).get

    override def bsonToPayload(doc: BSONDocument): SomeEvent =
      r.readDocument(doc).get
  }

}
