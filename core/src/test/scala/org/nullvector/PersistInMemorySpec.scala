package org.nullvector

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ActorTestKit, BehaviorTestKit, TestInbox}
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.persistence._
import com.typesafe.config.ConfigFactory
import org.nullvector.journal.InMemoryAsyncWriteJournal
import org.nullvector.snapshot.InMemorySnapshotStore
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.BSONDocument

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}
import scala.util.Random

class PersistInMemorySpec extends AnyFlatSpec with Matchers {

  import org.nullvector.PersistInMemory._

  val testKit: ActorTestKit                           = ActorTestKit()
  implicit val actorSystem: TypedActorSystem[Nothing] = testKit.system
  implicit val ec: ExecutionContextExecutor           = actorSystem.executionContext

  ReactiveMongoEventSerializer(actorSystem).addAdapter(
    EventAdapterFactory.adapt[StatusEvent]("MyEvent")
  )

  it should " add event in Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId           = "my-id"
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(
          EventEntry(
            persistenceId,
            1,
            "Event1",
            BSONDocument("x" -> "y"),
            Set.empty
          )
        ),
        Right(Promise[Done]())
      )
    )

    val replyEvents = TestInbox[Seq[EventWithOffset]]()
    persistInMemoryBehavior.run(EventsOf(persistenceId, Left(replyEvents.ref)))
    val events      = replyEvents.receiveMessage()
    events.head.eventEntry.event shouldBe BSONDocument("x" -> "y")
  }

  it should " add snapshot in Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId           = "my-id"
    persistInMemoryBehavior.run(
      Snapshot(
        persistenceId,
        SnapshotEntry(1, "Event1", BSONDocument("x" -> "y"), 1),
        Left(TestInbox[Done]().ref)
      )
    )

    val replySnapshot = TestInbox[Seq[SnapshotEntry]]()
    persistInMemoryBehavior.run(
      SnapshotsOf(persistenceId, Left(replySnapshot.ref))
    )
    val snapshot      = replySnapshot.receiveMessage()
    snapshot.head.event shouldBe BSONDocument("x" -> "y")
  }

  it should " remove event from Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId           = "my-id"
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(EventEntry(persistenceId, 1, "Event1", BSONDocument(), Set.empty)),
        Right(Promise[Done]())
      )
    )
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(
          EventEntry(
            persistenceId,
            2,
            "Event2",
            BSONDocument("a" -> "b"),
            Set.empty
          )
        ),
        Right(Promise[Done]())
      )
    )
    persistInMemoryBehavior.run(
      RemoveEventsOf(persistenceId, 1, Right(Promise[Done]()))
    )

    val replyEvents = TestInbox[Seq[EventWithOffset]]()
    persistInMemoryBehavior.run(EventsOf(persistenceId, Left(replyEvents.ref)))
    val events      = replyEvents.receiveMessage()
    events.head.eventEntry.event shouldBe BSONDocument("a" -> "b")
  }

  it should " max sequence from Memory" in {
    val persistInMemoryBehavior = BehaviorTestKit(PersistInMemory.behavior())
    val persistenceId           = "my-id"
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(EventEntry(persistenceId, 1, "Event1", BSONDocument(), Set.empty)),
        Right(Promise[Done]())
      )
    )
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(EventEntry(persistenceId, 2, "Event2", BSONDocument(), Set.empty)),
        Right(Promise[Done]())
      )
    )
    persistInMemoryBehavior.run(
      Persist(
        persistenceId,
        Seq(EventEntry(persistenceId, 3, "Event3", BSONDocument(), Set.empty)),
        Right(Promise[Done]())
      )
    )
    persistInMemoryBehavior.run(
      RemoveEventsOf(persistenceId, 1, Right(Promise[Done]()))
    )

    val replyMaxSeq = TestInbox[Long]()
    persistInMemoryBehavior.run(
      HighestSequenceOf(persistenceId, Left(replyMaxSeq.ref))
    )
    val maxSeq      = replyMaxSeq.receiveMessage()
    maxSeq shouldBe 3
  }

  it should " add event in Memory with Akka Extension" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId   = randomPersistenceId

    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 1, "Event1", BSONDocument(), Set.empty))
    )
    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 2, "Event2", BSONDocument(), Set.empty))
    )

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)
    events.size shouldBe 2
  }

  it should " add an Snapshot in Memory with Akka Extension" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId   = randomPersistenceId

    persistInMemory.addSnapshot(
      persistenceId,
      SnapshotEntry(1, "Snpsht", BSONDocument(), System.currentTimeMillis())
    )
    persistInMemory.addSnapshot(
      persistenceId,
      SnapshotEntry(1, "Snpsht", BSONDocument(), System.currentTimeMillis())
    )

    val events =
      Await.result(persistInMemory.snapshotsOf(persistenceId), 1.second)
    events.size shouldBe 2
  }

  it should " HighestSequenceOf in Memory with Akka Extension" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId   = randomPersistenceId

    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 23, "Event1", BSONDocument(), Set.empty))
    )
    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 25, "Event2", BSONDocument(), Set.empty))
    )

    val maxSeq =
      Await.result(persistInMemory.highestSequenceOf(persistenceId), 1.second)
    maxSeq shouldBe 25
  }

  it should " HighestSequenceOf in Memory with Akka Extension taking care snapshots" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId   = randomPersistenceId

    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 23, "Event1", BSONDocument(), Set.empty))
    )
    persistInMemory.addEvents(
      persistenceId,
      Seq(EventEntry(persistenceId, 25, "Event2", BSONDocument(), Set.empty))
    )
    persistInMemory.addSnapshot(
      persistenceId,
      SnapshotEntry(27, "Snpsht", BSONDocument(), 1)
    )
    persistInMemory.addSnapshot(
      persistenceId,
      SnapshotEntry(27, "Snpsht", BSONDocument(), 2)
    )

    val maxSeq =
      Await.result(persistInMemory.highestSequenceOf(persistenceId), 1.second)
    maxSeq shouldBe 27
  }

  it should " async write in memory" in {
    val persistInMemory = PersistInMemory(actorSystem)
    val persistenceId   = randomPersistenceId

    val asyncWriteJournal =
      new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    val units             = Await.result(
      asyncWriteJournal.asyncWriteMessages(
        Seq(
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId))
        )
      ),
      1.second
    )

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)

    units.size shouldBe 2
    events.size shouldBe 2
  }

  it should " async highest seq from Memory" in {
    val persistenceId     = randomPersistenceId
    val asyncWriteJournal =
      new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(
      asyncWriteJournal.asyncWriteMessages(
        Seq(
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId))
        )
      ),
      1.second
    )
    val highest           = Await.result(
      asyncWriteJournal.asyncReadHighestSequenceNr(persistenceId, 0),
      1.second
    )

    highest shouldBe 67
  }

  it should " async delete events seq from Memory" in {
    val persistInMemory   = PersistInMemory(actorSystem)
    val persistenceId     = randomPersistenceId
    val asyncWriteJournal =
      new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(
      asyncWriteJournal.asyncWriteMessages(
        Seq(
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 38, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId))
        )
      ),
      1.second
    )
    Await.result(
      asyncWriteJournal.asyncDeleteMessagesTo(persistenceId, 60),
      1.second
    )

    val events = Await.result(persistInMemory.eventsOf(persistenceId), 1.second)
    events.size shouldBe 1
  }

  it should " async replay from Memory" in {
    val persistenceId     = randomPersistenceId
    val asyncWriteJournal =
      new InMemoryAsyncWriteJournal(actorSystem.classicSystem)
    Await.result(
      asyncWriteJournal.asyncWriteMessages(
        Seq(
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 35, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 38, persistenceId)),
          AtomicWrite(PersistentRepr(StatusEvent("A Name"), 67, persistenceId))
        )
      ),
      1.second
    )
    val counter           = new AtomicInteger()
    Await.result(
      asyncWriteJournal.asyncReplayMessages(persistenceId, 30, 80, 20)(_ => counter.incrementAndGet()),
      1.second
    )

    counter.get() shouldBe 3
  }

  it should " load Snapshot Async" in {
    val snapshotStore = new InMemorySnapshotStore(actorSystem.classicSystem)
    val persistenceId = randomPersistenceId
    Await.ready(
      for {
        _ <- snapshotStore
               .saveAsync(SnapshotMetadata(persistenceId, 23, 1), BSONDocument())
        _ <- snapshotStore
               .saveAsync(SnapshotMetadata(persistenceId, 23, 2), BSONDocument())
        _ <- snapshotStore
               .saveAsync(SnapshotMetadata(persistenceId, 23, 3), BSONDocument())
      } yield (),
      1.second
    )

    val selected = Await
      .result(
        snapshotStore.loadAsync(persistenceId, SnapshotSelectionCriteria()),
        1.second
      )
      .get
    selected.metadata.timestamp shouldBe 3
  }

  it should " delete snapshots" in {
    val snapshotStore = new InMemorySnapshotStore(actorSystem.classicSystem)
    val persistenceId = randomPersistenceId
    Await.ready(
      for {
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 23, 1),
               BSONDocument()
             )
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 23, 2),
               BSONDocument()
             )
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 23, 3),
               BSONDocument()
             )
        _ <- snapshotStore.deleteAsync(SnapshotMetadata(persistenceId, 23))
      } yield (),
      1.second
    )

    val selected = Await.result(
      snapshotStore.loadAsync(persistenceId, SnapshotSelectionCriteria()),
      1.second
    )
    selected shouldBe None
  }

  it should " delete snapshots with criteria" in {
    val snapshotStore = new InMemorySnapshotStore(actorSystem.classicSystem)
    val persistenceId = randomPersistenceId
    Await.ready(
      for {
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 23, 1),
               BSONDocument()
             )
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 24, 2),
               BSONDocument()
             )
        _ <- snapshotStore.saveAsync(
               SnapshotMetadata(persistenceId, 25, 3),
               BSONDocument()
             )
        _ <- snapshotStore.deleteAsync(
               persistenceId,
               SnapshotSelectionCriteria(minSequenceNr = 24L)
             )
      } yield (),
      1.second
    )

    val selected = Await
      .result(
        snapshotStore.loadAsync(persistenceId, SnapshotSelectionCriteria()),
        1.second
      )
      .get
    selected.metadata.sequenceNr shouldBe 23
  }

  it should "load from configuration and integrate all" in {
    val config = ConfigFactory.parseString(
      """
        |  akka-persistence-reactivemongo.persist-in-memory = true
      """.stripMargin
    ) withFallback ConfigFactory.load()

    val inMemoryAS    = ActorSystem("InMemoryAS", config)
    ReactiveMongoEventSerializer(inMemoryAS.toTyped).addAdapter(
      EventAdapterFactory.adapt[StatusEvent]("MyEvent")
    )
    val pId           = "InMemory-1"
    val persistorRef1 = inMemoryAS.actorOf(persistorProps(pId))

    val promisedDone = Promise[Done]()
    persistorRef1 ! ChangeStatus(
      Seq(StatusEvent("Greetings"), StatusEvent("From"), StatusEvent("Earth")),
      promisedDone
    )
    Await.result(promisedDone.future, 1.second)
    persistorRef1 ! PoisonPill

    val persistorRef2  = inMemoryAS.actorOf(persistorProps(pId))
    val promisedString = Promise[String]()
    persistorRef2 ! GetStatus(promisedString)
    Await.result(promisedString.future, 1.second) shouldBe "Earth"

    Await.ready(inMemoryAS.terminate(), 2.seconds)
  }

  it should "load snapshot from configuration and integrate all" in {
    val config = ConfigFactory.parseString(
      """
        |  akka-persistence-reactivemongo.persist-in-memory = true
      """.stripMargin
    ) withFallback ConfigFactory.load()

    val inMemoryAS    = ActorSystem("InMemoryAS", config)
    ReactiveMongoEventSerializer(inMemoryAS.toTyped).addAdapter(
      EventAdapterFactory.adapt[StatusEvent]("MyEvent")
    )
    ReactiveMongoEventSerializer(inMemoryAS.toTyped).addAdapter(
      EventAdapterFactory.adapt[Status]("StatusSnapshot")
    )
    val pId           = "InMemory-1"
    val persistorRef1 = inMemoryAS.actorOf(persistorProps(pId))

    {
      persistorRef1 ! ChangeStatus(
        Seq(StatusEvent("Greetings")),
        Promise[Done]()
      )
      val promisedDone = Promise[Done]()
      persistorRef1 ! SnapshotStatus(promisedDone)
      Await.result(promisedDone.future, 1.second)
    }
    Thread.sleep(50)
    val snapshot = Await
      .result(
        new InMemorySnapshotStore(inMemoryAS)
          .loadAsync(pId, SnapshotSelectionCriteria()),
        1.second
      )
      .get
    snapshot.snapshot shouldBe Status("Greetings")

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

    override def journalPluginId: String =
      "akka-persistence-reactivemongo-journal"

    override def snapshotPluginId: String =
      "akka-persistence-reactivemongo-snapshot"

    override def receiveRecover: Receive = {
      case StatusEvent(anStatus)              => status = anStatus
      case SnapshotOffer(_, snapshot: Status) => status = snapshot.status
    }

    override def receiveCommand: Receive = {
      case ChangeStatus(events, promise) =>
        persistAll(events)(event => status = event.status)
        defer(None)(_ => promise.success(Done))
      case GetStatus(promise)            => promise.success(status)
      case SnapshotStatus(promise)       =>
        saveSnapshot(Status(status))
        promise.success(Done)

    }

    override def persistenceId: String = id

  })
}
