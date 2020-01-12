package org.nullvector.journal

import akka.Done
import akka.actor.{ActorSystem, Kill, PoisonPill, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.Tagged
import akka.testkit.{ImplicitSender, TestKit}
import org.nullvector.{EventAdapter, ReactiveMongoEventSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler, Macros}
import util.AutoRestartFactory

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.util.Random

class PersistentActorSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  private val serializer = ReactiveMongoEventSerializer(system)
  private val autoRestartFactory = new AutoRestartFactory(system)

  serializer.addEventAdapter(new AnEventEventAdapter)

  def randomId: Long = Random.nextLong().abs

  "A Persistent Actor" should {

    "Persist Events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(13.seconds, None)

      actorRef ! Command("Command1")
      actorRef ! Command("Command2")
      actorRef ! Command("Command3")
      actorRef ! Command("Command4")
      actorRef ! Command("Command5")
      actorRef ! Command("Command6")
      actorRef ! Command("Command7")
      receiveN(7, 15.seconds)

      actorRef ! Command("get_state")
      expectMsg(Some("Command7"))
    }

    "PersistAll" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! MultiCommand("Action One", "Action Two", "Action Three")
      receiveN(1, 15.seconds)
      actorRef ! Kill
      actorRef ! Command("get_state")
      expectMsg(15.seconds, Some("Action Three"))
    }

    "Recover Events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("Event One")
      expectMsg(Done)
      actorRef ! Command("Event Two")
      expectMsg(Done)
      actorRef ! Kill
      actorRef ! Command("get_state")
      expectMsg(15.seconds, Some("Event Two"))
    }

    "Delete events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(15.seconds, None)

      actorRef ! Command("Event One")
      actorRef ! Command("Event Two")
      Thread.sleep(1000) //Give some time to delete messages
      actorRef ! Command("delete")
      receiveN(3, 15.seconds)
      Thread.sleep(1000) //Give some time to delete messages
      actorRef ! Kill
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(15.seconds, None)

    }

  }

  override def afterAll: Unit = {
    shutdown()
  }

  case class Command(action: Any)

  case class MultiCommand(action1: String, action2: String, action3: String)

  case class AnEvent(string: String)

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: Option[String] = None

    override def receiveCommand: Receive = {
      case Command("get_state") => sender() ! state

      case Command("delete") =>
        state = None
        deleteMessages(lastSequenceNr)
        sender() ! state

      case Command(action) =>
        println(s"Will persist action $action")
        persistAsync(AnEvent(action.toString)) { event =>
          println(s"Event $event persisted")
          state = Some(event.string)
          sender() ! Done
        }

      case MultiCommand(action1, action2, action3) =>
        println(s"Will persist MultiCommand")

        persistAll(Seq(AnEvent(action1), AnEvent(action2), AnEvent(action3))) { _ => }
        deferAsync(()) { _ =>
          println(s"All Events persisted")
          state = Some(action3)
          sender() ! Done
        }

    }

    override def receiveRecover: Receive = {
      case AnEvent(string) => state = Some(string)
    }
  }


  class AnEventEventAdapter extends EventAdapter[AnEvent] {
    override val manifest: String = "AnEvent"

    override def tags(payload: Any): Set[String] = Set("tag_1", "tag_2")

    private val anEventMapper: BSONDocumentHandler[AnEvent] = Macros.handler[AnEvent]

    override def payloadToBson(payload: AnEvent): BSONDocument = anEventMapper.write(payload)

    override def bsonToPayload(doc: BSONDocument): AnEvent = anEventMapper.read(doc)
  }

}