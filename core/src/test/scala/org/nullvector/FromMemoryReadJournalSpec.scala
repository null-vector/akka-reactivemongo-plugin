package org.nullvector

import java.util.concurrent.atomic.AtomicInteger
import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.ConfigFactory
import org.nullvector.PersistInMemory.EventEntry
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal, RefreshInterval}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.scalatest.{FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.Random

class FromMemoryReadJournalSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "akka-persistence-reactivemongo-journal"
      |akka.persistence.snapshot-store.plugin = "akka-persistence-reactivemongo-snapshot"
      |akka-persistence-reactivemongo.persist-in-memory = true
      """.stripMargin
  ) withFallback ConfigFactory.load()

  val testKit: ActorTestKit = ActorTestKit(config)
  implicit val actorSystem: TypedActorSystem[Nothing] = testKit.system
  implicit val ec: ExecutionContextExecutor = actorSystem.executionContext

  private val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(actorSystem)
  serializer.addAdapter(EventAdapterFactory.adapt[AnEvent]("AnEvent", (event: AnEvent) => Set(event.tag)))
  private val readJournal: ReactiveMongoScalaReadJournal = ReactiveMongoJournalProvider(actorSystem).scaladslReadJournal
  private val memory: PersistInMemory = PersistInMemory(actorSystem)


  it should " current by Tags " in {
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId1", Seq(EventEntry("pId1", 1, "AnEvent", BSONDocument("tag" -> "tagA"), Set("tagA"))))
      _ <- memory.addEvents("pId2", Seq(EventEntry("pId2", 1, "AnEvent", BSONDocument("tag" -> "tagB"), Set("tagB"))))
      _ <- memory.addEvents("pId3", Seq(EventEntry("pId3", 1, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)

    val eventSource = readJournal.currentEventsByTags(Seq("tagA", "tagB"), NoOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(_, _, _, AnEvent("tagA")) => true
      case EventEnvelope(_, _, _, AnEvent("tagB")) => true
    } shouldBe 2
  }

  it should " current by Tag " in {
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId1", Seq(EventEntry("pId1", 1, "AnEvent", BSONDocument("tag" -> "tagA"), Set("tagA"))))
      _ <- memory.addEvents("pId2", Seq(EventEntry("pId2", 1, "AnEvent", BSONDocument("tag" -> "tagB"), Set("tagB"))))
      _ <- memory.addEvents("pId3", Seq(EventEntry("pId3", 1, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)

    val eventSource = readJournal.currentEventsByTag("tagA", NoOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(_, _, _, AnEvent("tagA")) => true
    } shouldBe 1
  }

  it should " current by Tags and offset " in {
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId6", Seq(EventEntry("pId6", 6, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
      _ <- memory.addEvents("pId7", Seq(EventEntry("pId7", 5, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
      _ <- memory.addEvents("pId8", Seq(EventEntry("pId8", 4, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)

    val fromOffset = Await.result(memory.eventsOf("pId7"), 1.second).head.offset
    val remainOffset = Await.result(memory.eventsOf("pId8"), 1.second).head.offset

    val eventSource = readJournal.currentEventsByTags(Seq("tagC"), fromOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(`remainOffset`, "pId8", 4, AnEvent("tagC")) => true
    } shouldBe 1
  }

  it should " current by Tags and offset get raw events" in {
    val document = BSONDocument("tag" -> "tagC3")
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId6", Seq(EventEntry("pId6", 6, "AnEvent", BSONDocument("tag" -> "tagC1"), Set("tagC"))))
      _ <- memory.addEvents("pId7", Seq(EventEntry("pId7", 5, "AnEvent", BSONDocument("tag" -> "tagC2"), Set("tagC"))))
      _ <- memory.addEvents("pId8", Seq(EventEntry("pId8", 4, "AnEvent", document, Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)
    val fromOffset = Await.result(memory.eventsOf("pId7"), 1.second).head.offset
    val remainOffset = Await.result(memory.eventsOf("pId8"), 1.second).head.offset

    val eventSource = readJournal.currentRawEventsByTag(Seq("tagC"), fromOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(`remainOffset`, "pId8", 4, `document`) => true
    } shouldBe 1
  }

  it should " current by a single Tag and offset get raw events" in {
    val document = BSONDocument("tag" -> "tagC3")
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId6", Seq(EventEntry("pId6", 6, "AnEvent", BSONDocument("tag" -> "tagC1"), Set("tagC"))))
      _ <- memory.addEvents("pId7", Seq(EventEntry("pId7", 5, "AnEvent", BSONDocument("tag" -> "tagC2"), Set("tagC"))))
      _ <- memory.addEvents("pId8", Seq(EventEntry("pId8", 4, "AnEvent", document, Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)
    val fromOffset = Await.result(memory.eventsOf("pId7"), 1.second).head.offset
    val remainOffset = Await.result(memory.eventsOf("pId8"), 1.second).head.offset

    val eventSource = readJournal.currentRawEventsByTag("tagC", fromOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(`remainOffset`, "pId8", 4, `document`) => true
    } shouldBe 1
  }

  it should " current persistence Ids" in {
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId8", Seq(EventEntry("pId8", 6, "AnEvent", BSONDocument(), Set.empty)))
      _ <- memory.addEvents("pId7", Seq(EventEntry("pId7", 5, "AnEvent", BSONDocument(), Set.empty)))
      _ <- memory.addEvents("pId6", Seq(EventEntry("pId6", 4, "AnEvent", BSONDocument(), Set.empty)))
    } yield ()
    Await.result(addEvents, 1.second)

    val ids = readJournal.currentPersistenceIds()
    val envelopes = Await.result(ids.runWith(Sink.seq), 1.second)

    envelopes should contain theSameElementsAs Seq("pId6", "pId8", "pId7")
  }

  it should " current events by persistence Id" in {
    val aPersistenceId = "pId87"
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents(aPersistenceId, Seq(
        EventEntry(aPersistenceId, 6, "AnEvent", BSONDocument("tag" -> ""), Set.empty),
        EventEntry(aPersistenceId, 7, "AnEvent", BSONDocument("tag" -> ""), Set.empty),
        EventEntry(aPersistenceId, 8, "AnEvent", BSONDocument("tag" -> ""), Set.empty),
        EventEntry(aPersistenceId, 9, "AnEvent", BSONDocument("tag" -> ""), Set.empty),
      ))
    } yield ()
    Await.result(addEvents, 1.second)

    val ids = readJournal.currentEventsByPersistenceId(aPersistenceId, 6, 9)
    val envelopes = Await.result(ids.runWith(Sink.seq), 1.second)

    envelopes.map(_.sequenceNr) should contain theSameElementsAs Seq(7, 8, 9)
  }

  it should " infinite event by tag stream" in {
    Await.result(memory.invalidateAll(), 1.second)
    val atomicInt = new AtomicInteger(0)
    val promisedDone = Promise[Done]()
    val streamKiller = readJournal
      .eventsByTag("Infinite", NoOffset)
      .addAttributes(RefreshInterval(2.millis))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach { event =>
        val counter = atomicInt.incrementAndGet()
        if (counter == 55) promisedDone.success(Done)
      })(Keep.left)
      .run()

    val pid = s"pid-${randomPid}"

    val eventualDone = Source(1 to 55).async.runForeach { seq =>
      Thread.sleep(1)
      memory.addEvents(pid, Seq(EventEntry(pid, seq, "AnEvent", BSONDocument("tag" -> s"$seq"), Set("Infinite"))))
    }
    Await.result(eventualDone, 1.second)

    try {
      Await.result(promisedDone.future, 1.seconds)
    } finally {
      streamKiller.shutdown()
    }
    atomicInt.get() shouldBe 55
  }

  private def randomPid: String = Random.nextInt().abs.toString

  case class AnEvent(tag: String)

}