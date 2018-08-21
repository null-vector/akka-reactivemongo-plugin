package org.nullvector.journal

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.Tagged
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class PersistentActorSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  "A Persistent Actor" should {

    "Persist Events" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("3456")))
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      actorRef ! Command("Command1")
      receiveN(7, 7.seconds)
    }

    "Persist Events with Tags" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("tags-3456")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      actorRef ! Command(Tagged("Command2", Set("tag_1", "tag_2")))
      receiveN(7, 7.seconds)
    }

    "Recover Events" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("recover-876")))
      actorRef ! Command("Event One")
      actorRef ! Command("Event Two")
      receiveN(2, 7.seconds)
      actorRef ! PoisonPill
      Thread.sleep(1000)
      system.actorOf(Props(new SomePersistentActor("recover-876"))) ! "get_state"
      expectMsg(7.seconds, "Event Two")
    }

  }

  override def afterAll {
    shutdown()
  }

  case class Command(event: Any)

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: String = ""

    override def receiveCommand: Receive = {
      case "get_state" => sender() ! state
      case Command(event) => persistAsync(event) { event =>
        state = event.toString
        sender() ! "ok"
      }
    }

    override def receiveRecover: Receive = {
      case event: String => state = event
    }
  }

}