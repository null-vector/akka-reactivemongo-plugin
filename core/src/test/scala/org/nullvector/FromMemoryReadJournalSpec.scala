package org.nullvector

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ActorTestKit, BehaviorTestKit, TestInbox}
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.persistence._
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.nullvector.PersistInMemory.EventEntry
import org.nullvector.journal.InMemoryAsyncWriteJournal
import org.nullvector.query.{ObjectIdOffset, ReactiveMongoJournalProvider, ReactiveMongoScalaReadJournal}
import org.nullvector.snapshot.InMemorySnapshotStore
import org.scalatest.{FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}
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
  serializer.addEventAdapter(EventAdapterFactory.adapt[AnEvent]("AnEvent", (event: AnEvent) => Set(event.tag)))
  private val readJournal: ReactiveMongoScalaReadJournal = ReactiveMongoJournalProvider(actorSystem).scaladslReadJournal
  private val memory: PersistInMemory = PersistInMemory(actorSystem)


  it should " current by Tags " in {
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId1", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagA"), Set("tagA"))))
      _ <- memory.addEvents("pId2", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagB"), Set("tagB"))))
      _ <- memory.addEvents("pId3", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
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
      _ <- memory.addEvents("pId1", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagA"), Set("tagA"))))
      _ <- memory.addEvents("pId2", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagB"), Set("tagB"))))
      _ <- memory.addEvents("pId3", Seq(EventEntry(1, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"))))
    } yield ()
    Await.result(addEvents, 1.second)

    val eventSource = readJournal.currentEventsByTag("tagA", NoOffset)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(_, _, _, AnEvent("tagA")) => true
    } shouldBe 1
  }

  it should " current by Tags and offset " in {
    val (offset1, offset2, offset3) = (ObjectIdOffset.newOffset(), ObjectIdOffset.newOffset(), ObjectIdOffset.newOffset())
    val addEvents = for {
      _ <- memory.invalidateAll()
      _ <- memory.addEvents("pId6", Seq(EventEntry(6, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"), Some(offset1))))
      _ <- memory.addEvents("pId7", Seq(EventEntry(5, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"), Some(offset2))))
      _ <- memory.addEvents("pId8", Seq(EventEntry(4, "AnEvent", BSONDocument("tag" -> "tagC"), Set("tagC"), Some(offset3))))
    } yield ()
    Await.result(addEvents, 1.second)

    val eventSource = readJournal.currentEventsByTags(Seq("tagC"), offset2)
    val envelopes = Await.result(eventSource.runWith(Sink.seq), 1.second)

    envelopes.count {
      case EventEnvelope(`offset3`, _, 4, AnEvent("tagC")) => true
    } shouldBe 1
  }

  private def randomPersistenceId: String = Random.nextLong().abs.toString

  case class AnEvent(tag: String)


}