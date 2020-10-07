package org.nullvector.journal

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.actor.testkit.typed.scaladsl.{ActorTestKit, BehaviorTestKit, TestInbox}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Extension, ExtensionId, ActorSystem => TypedActorSystem}
import akka.persistence.{AtomicWrite, PersistentActor, PersistentRepr, SnapshotOffer}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.nullvector.journal.inmemory.{InMemoryAsyncWriteJournal, PersistInMemory}
import org.nullvector.query.ObjectIdOffset
import org.nullvector.{EventAdapterFactory, ReactiveMongoEventSerializer}
import org.scalatest.{FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Random, Success, Try}

class PersistInMemorySpec extends FlatSpec with Matchers {

  import org.nullvector.journal.inmemory.PersistInMemory._

  val testKit: ActorTestKit = ActorTestKit()
  implicit val actorSystem: TypedActorSystem[Nothing] = testKit.system

  ReactiveMongoEventSerializer(actorSystem).addEventAdapter(EventAdapterFactory.adapt[StatusEvent]("MyEvent"))

  it should " add event in Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId = "my-id"
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(1, "Event1", BSONDocument("x" -> "y"), Set.empty))))

    val replyEvents = TestInbox[Seq[EventEntry]]()
    persistInMemoryBehavior.run(EventsOf(persistenceId, Left(replyEvents.ref)))
    val events = replyEvents.receiveMessage()
    events.head.event shouldBe BSONDocument("x" -> "y")
  }

  it should " add snapshot in Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId = "my-id"
    persistInMemoryBehavior.run(Snapshot(persistenceId, SnapshotEntry(1, "Event1", BSONDocument("x" -> "y"), 1)))

    val replySnapshot = TestInbox[Seq[SnapshotEntry]]()
    persistInMemoryBehavior.run(SnapshotsOf(persistenceId, Left(replySnapshot.ref)))
    val snapshot = replySnapshot.receiveMessage()
    snapshot.head.event shouldBe BSONDocument("x" -> "y")
  }

  it should " remove event from Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId = "my-id"
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(1, "Event1", BSONDocument(), Set.empty))))
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(2, "Event2", BSONDocument("a" -> "b"), Set.empty))))
    persistInMemoryBehavior.run(RemoveEventsOf(persistenceId, 1))

    val replyEvents = TestInbox[Seq[EventEntry]]()
    persistInMemoryBehavior.run(EventsOf(persistenceId, Left(replyEvents.ref)))
    val events = replyEvents.receiveMessage()
    events.head.event shouldBe BSONDocument("a" -> "b")
  }

  it should " max sequence from Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId = "my-id"
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(1, "Event1", BSONDocument(), Set.empty))))
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(2, "Event2", BSONDocument(), Set.empty))))
    persistInMemoryBehavior.run(Persist(persistenceId, Seq(EventEntry(3, "Event3", BSONDocument(), Set.empty))))
    persistInMemoryBehavior.run(RemoveEventsOf(persistenceId, 1))

    val replyMaxSeq = TestInbox[Long]()
    persistInMemoryBehavior.run(HighestSequenceOf(persistenceId, Left(replyMaxSeq.ref)))
    val maxSeq = replyMaxSeq.receiveMessage()
    maxSeq shouldBe 3
  }

  it should " add event in Memory with Akka Extension" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId = randomPersistenceId

    persistInMemory.addEvents(persistenceId, Seq(EventEntry(1, "Event1", BSONDocument(), Set.empty)))
    persistInMemory.addEvents(persistenceId, Seq(EventEntry(2, "Event2", BSONDocument(), Set.empty)))

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)
    events.size shouldBe 2

  }

  it should " HighestSequenceOf in Memory with Akka Extension" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId = randomPersistenceId

    persistInMemory.addEvents(persistenceId, Seq(EventEntry(23, "Event1", BSONDocument(), Set.empty)))
    persistInMemory.addEvents(persistenceId, Seq(EventEntry(25, "Event2", BSONDocument(), Set.empty)))

    val maxSeq = Await.result(persistInMemory.highestSequenceOf(persistenceId), 1.second)
    maxSeq shouldBe 25

  }

  it should " async write in memory" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId = randomPersistenceId

    val asyncWriteJournal = new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    val units = Await.result(asyncWriteJournal.asyncWriteMessages(Seq(
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId)),
    )), 1.second)

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)

    units.size shouldBe 2
    events.size shouldBe 2
  }

  it should " async highest seq from Memory" in {
    val persistenceId = randomPersistenceId
    val asyncWriteJournal = new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(asyncWriteJournal.asyncWriteMessages(Seq(
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId)),
    )), 1.second)
    val highest = Await.result(asyncWriteJournal.asyncReadHighestSequenceNr(persistenceId, 0), 1.second)

    highest shouldBe 67
  }

  it should " async delete events seq from Memory" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId = randomPersistenceId
    val asyncWriteJournal = new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(asyncWriteJournal.asyncWriteMessages(Seq(
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 38, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId)),
    )), 1.second)
    Await.result(asyncWriteJournal.asyncDeleteMessagesTo(persistenceId, 60), 1.second)

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)
    events.size shouldBe 1
  }

  it should " async replay from Memory" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId = randomPersistenceId
    val asyncWriteJournal = new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(asyncWriteJournal.asyncWriteMessages(Seq(
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 38, persistenceId)),
      AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId)),
    )), 1.second)
    val counter = new AtomicInteger()
    Await.result(asyncWriteJournal.asyncReplayMessages(persistenceId, 30, 80, 20)(_ => counter.incrementAndGet()), 1.second)

    counter.get() shouldBe 3
  }

  it should "load from configuration and integrate all" in {
    val config = ConfigFactory.parseString(
      """
        |  akka-persistence-reactivemongo.persist-in-memory = true
      """.stripMargin
    ) withFallback ConfigFactory.load()

    val inMemoryAS = ActorSystem("InMemoryAS", config)
    ReactiveMongoEventSerializer(inMemoryAS).addEventAdapter(EventAdapterFactory.adapt[StatusEvent]("MyEvent"))
    val pId = "InMemory-1"
    val persistorRef1 = inMemoryAS.actorOf(persistorProps(pId))

    val promisedDone = Promise[Done]()
    persistorRef1 ! ChangeStatus(Seq(StatusEvent("Greetings"), StatusEvent("From"), StatusEvent("Earth")), promisedDone)
    Await.result(promisedDone.future, 1.second)
    persistorRef1 ! PoisonPill

    val persistorRef2 = inMemoryAS.actorOf(persistorProps(pId))
    val promisedString = Promise[String]()
    persistorRef2 ! GetStatus(promisedString)
    Await.result(promisedString.future, 1.second) shouldBe "Earth"

    Await.ready(inMemoryAS.terminate(), 2.seconds)
  }

  private def randomPersistenceId: String = Random.nextLong().abs.toString

  case class StatusEvent(status: String)
  case class Status(status: String)

  case class ChangeStatus(events: Seq[StatusEvent], response: Promise[Done])
  case class SnapshotStatus(response: Promise[Done])

  case class GetStatus(response: Promise[String])

  def persistorProps(id: String) = Props(new PersistentActor {
    var status: String = ""

    override def journalPluginId: String = "akka-persistence-reactivemongo-journal"

    override def receiveRecover: Receive = {
      case StatusEvent(anStatus) => status = anStatus
      case SnapshotOffer(_, snapshot: Status) => status = snapshot.status
    }

    override def receiveCommand: Receive = {
      case ChangeStatus(events, promise) =>
        persistAll(events)(event => status = event.status)
        defer(None)(_ => promise.success(Done))
      case GetStatus(promise) => promise.success(status)
      case SnapshotStatus(promise) =>
        saveSnapshot(Status(status))
        promise.success(Done)
    }

    override def persistenceId: String = id

  })
}