package org.nullvector.journal

import akka.actor.{ActorSystem, Kill, PoisonPill, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.Tagged
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import util.AutoRestartFactory

import scala.concurrent.duration._
import scala.util.Random

class PersistentActorSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  private val serializer = ReactiveMongoEventSerializer(system)
  private val autoRestartFactory = new AutoRestartFactory(system)

  def randomId: Long = Random.nextLong().abs

  "A Persistent Actor" should {

    "Persist Events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(7.seconds, None)

      actorRef ! Command("Command1")
      actorRef ! Command("Command2")
      actorRef ! Command("Command3")
      actorRef ! Command("Command4")
      actorRef ! Command("Command5")
      actorRef ! Command("Command6")
      actorRef ! Command("Command7")
      receiveN(7, 7.seconds)

      actorRef ! Command("get_state")
      expectMsg(Some("Command7"))
    }

    "Persist Events with Tags" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command1", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command3", Set("tag_1", "tag_2")))
      receiveN(3, 7.seconds)
    }

    "Recover Events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("Event One")
      actorRef ! Command("Event Two")
      receiveN(2, 7.seconds)
      actorRef ! Kill
      actorRef ! Command("get_state")
      expectMsg(7.seconds, Some("Event Two"))
    }

    "Delete events" in {
      val persistId = randomId.toString
      val actorRef = autoRestartFactory.create(Props(new SomePersistentActor(persistId)), persistId)
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(7.seconds, None)

      actorRef ! Command("Event One")
      actorRef ! Command("Event Two")
      Thread.sleep(1000) //Give some time to delete messages
      actorRef ! Command("delete")
      receiveN(3, 7.seconds)
      Thread.sleep(1000) //Give some time to delete messages
      actorRef ! Kill
      actorRef ! Command("get_state") //Will recover Nothing
      expectMsg(7.seconds, None)

    }

  }

  override def afterAll {
    shutdown()
  }

  case class Command(event: Any)

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: Option[String] = None

    override def receiveCommand: Receive = {
      case Command("get_state") => sender() ! state

      case Command("delete") =>
        state = None
        deleteMessages(lastSequenceNr)
        sender() ! state

      case Command(event) =>
        println(s"Will persist event $event")
        persistAsync(event) { event =>
          println(s"Event $event persisted")
          state = Some(event.toString)
          sender() ! "ok"
        }
    }

    override def receiveRecover: Receive = {
      case event: String => state = Some(event)
    }
  }

}