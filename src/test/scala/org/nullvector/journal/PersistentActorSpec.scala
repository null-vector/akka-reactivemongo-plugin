package org.nullvector.journal

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.persistence.{AtomicWrite, PersistentActor, PersistentRepr}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.Random
import scala.concurrent.duration._

class PersistentActorSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  "A Persistent Actor" should {
    "Persist Events" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("3456")))
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      actorRef ! "Some String command"
      receiveN(7, 7.seconds)
    }

    "Recover Events" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("876")))
      actorRef ! "Event One"
      actorRef ! "Event Two"
      receiveN(2, 7.seconds)
      actorRef ! PoisonPill
      Thread.sleep(1000)
      system.actorOf(Props(new SomePersistentActor("876"))) ! "get_state"
      expectMsg(7.seconds, "Event Two")
    }

  }

  override def afterAll {
    shutdown()
  }

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: String = ""

    override def receiveCommand: Receive = {
      case "get_state" => sender() ! state
      case command: String => persistAsync(command) { event =>
        state = event
        sender() ! "ok"
      }

    }

    override def receiveRecover: Receive = {
      case event: String => state = event
    }
  }

}