package org.nullvector.journal

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ExtendedActorSystem, Kill, Props, typed}
import akka.persistence.{PersistentActor, SnapshotOffer}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{ImplicitSender, TestKitBase}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.nullvector.{EventAdapter, EventAdapterFactory, ReactiveMongoDriver}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}
import util.AutoRestartFactory

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random

class PersistentActorSpec() extends TestKitBase with ImplicitSender with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  override lazy implicit val system =
    typed.ActorSystem(Behaviors.empty, "ReactiveMongoPlugin").classicSystem

  val serializer                                    = ReactiveMongoEventSerializer(system.toTyped)
  val autoRestartFactory                            = new AutoRestartFactory(
    system.asInstanceOf[ExtendedActorSystem]
  )
  val rxDriver: ReactiveMongoDriver                 = ReactiveMongoDriver(system)
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher
  serializer.addAdapter(new AnEventEventAdapter)
  serializer.addAdapter(new AFaultyEventAdapter)

  def randomId: Long = Random.nextLong().abs

  "A Persistent Actor" should {

    "Persist Events" in {
      val persistId = randomId.toString

      val actorRef = autoRestartFactory.create(
        Props(new SomePersistentActor(persistId)),
        persistId
      )
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(13.seconds, None)

      actorRef ! Command("A")
      actorRef ! CommandAll(Seq("B", "C", "D"))
      actorRef ! Command("E")
      actorRef ! CommandAll(Seq("F", "G", "H"))
      actorRef ! Command("I")
      receiveN(4, 15.seconds)

      actorRef ! Kill

      Thread.sleep(1000)

      actorRef ! Command("get_state")
      expectMsg(Some("I"))
    }

    "PersistAll" in {
      val persistId = randomId.toString
      val actorRef  = autoRestartFactory.create(
        Props(new SomePersistentActor(persistId)),
        persistId
      )
      actorRef ! MultiCommand("Action One", "Action Two", "Action Three")
      receiveN(1, 7.seconds)
      actorRef ! Kill
      actorRef ! Command("get_state")
      expectMsg(7.seconds, Some("Action Three"))
    }

    "Rea will fail" in {
      val persistId = randomId.toString
      val actorRef  = autoRestartFactory.create(
        Props(new SomePersistentActor(persistId)),
        persistId
      )
      actorRef ! Command("save_faulty_event")
      expectMsg(7.seconds, Done)
      actorRef ! Kill
      actorRef ! Command("get_state")
      expectNoMessage(1.seconds)
    }

    "Recover Events" in {
      val persistId = s"Zeta-${Random.nextInt().abs}"
      val actorRef  = autoRestartFactory.create(
        Props(new SomePersistentActor(persistId)),
        persistId
      )

      {
        val amountOfEvents = 1657
        val eventualDone   = Source(1 to amountOfEvents).runForeach(aNumber => {
          actorRef ! Command(s"Event One ($aNumber)")
        })
        Await.result(eventualDone, 7.seconds)
        receiveN(amountOfEvents, 21.seconds)
        actorRef ! Command("Event Two")
        expectMsg(Done)
        actorRef ! Kill
      }

      actorRef ! Command("get_state")
      expectMsg(7.seconds, Some("Event Two"))
    }

    "Delete events" in {
      val persistId = randomId.toString
      val actorRef  = autoRestartFactory.create(
        Props(new SomePersistentActor(persistId)),
        persistId
      )
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(15.seconds, None)

      actorRef ! Command("Event One")
      actorRef ! Command("Event Two")
      Thread.sleep(200)               //Give some time to delete messages
      actorRef ! Command("delete")
      receiveN(3, 15.seconds)
      Thread.sleep(200)               //Give some time to delete messages
      actorRef ! Kill
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(15.seconds, None)

    }

  }

  override def afterAll(): Unit = {
    shutdown()
  }

  private def dropAll(prefix: Option[String] = None) = {
    Await.result(
      Source
        .future(rxDriver.journals())
        .mapConcat(colls => prefix.fold(colls)(x => colls.filter(_.name == x)))
        .mapAsync(1)(_.drop(failIfNotFound = false))
        .runWith(Sink.ignore),
      14.seconds
    )
  }

  case class Command(action: Any)

  case class CommandAll(actions: Seq[Any])

  case class MultiCommand(action1: String, action2: String, action3: String)

  case class AnEvent(string: String)

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: Option[String] = None

    override def receiveCommand: Receive = {
      case Command("get_state") => sender() ! state

      case Command("save_faulty_event") =>
        persist(FaultyEvent("will fail"))(_ => sender() ! Done)

      case Command("delete") =>
        state = None
        deleteMessages(lastSequenceNr)
        sender() ! state

      case Command(action) =>
        persist(AnEvent(action.toString)) { event =>
          state = Some(event.string)
          sender() ! Done
          if (lastSequenceNr % 13 == 0) saveSnapshot(AnEvent(action.toString))
        }

      case CommandAll(actions) =>
        persistAll(actions.map(a => AnEvent(a.toString))) { event => }
        defer(actions) { _ =>
          sender() ! Done
          state = Some(actions.last.toString)
        }

      case MultiCommand(action1, action2, action3) =>
        persistAll(Seq(AnEvent(action1), AnEvent(action2), AnEvent(action3))) { _ => }
        deferAsync(()) { _ =>
          state = Some(action3)
          sender() ! Done
        }

    }

    override def receiveRecover: Receive = {
      case AnEvent(string)                  => state = Some(string)
      case SnapshotOffer(_, event: AnEvent) => state = Some(event.string)
    }
  }

  class AnEventEventAdapter extends EventAdapter[AnEvent] {
    override val manifest: String = "AnEvent"

    override def tags(payload: AnEvent): Set[String] = Set("tag_1", "tag_2")

    private val anEventMapper: BSONDocumentHandler[AnEvent] =
      Macros.handler[AnEvent]

    override def payloadToBson(payload: AnEvent): BSONDocument =
      anEventMapper.writeTry(payload).get

    override def bsonToPayload(doc: BSONDocument): AnEvent =
      anEventMapper.readDocument(doc).get
  }

  case class FaultyEvent(property1: String)

  class AFaultyEventAdapter extends EventAdapter[FaultyEvent] {
    override val manifest: String = "FaultyEvent"

    override def tags(payload: FaultyEvent): Set[String] = Set.empty

    private val mapping = EventAdapterFactory.mappingOf[FaultyEvent]

    override def payloadToBson(payload: FaultyEvent): BSONDocument =
      BSONDocument("faultyField" -> "boom")

    override def bsonToPayload(doc: BSONDocument): FaultyEvent     =
      mapping.readDocument(doc).get
  }

}
