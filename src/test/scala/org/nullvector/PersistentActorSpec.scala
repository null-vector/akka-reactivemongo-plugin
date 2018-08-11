package org.nullvector

import akka.actor.{ActorSystem, Props}
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
    "xxx" in {
      val actorRef = system.actorOf(Props(new SomePersistentActor("3456")))
      actorRef ! "Some String command"
      expectMsg("ok")
    }
  }

  override def afterAll {
    shutdown()
  }

  class SomePersistentActor(id: String) extends PersistentActor {
    override def persistenceId: String = s"SomeCollection-$id"

    var state: String = ""

    override def receiveCommand: Receive = {
      case command: String => persistAsync(command) { event =>
        state = event
        sender() ! "ok"
      }
    }

    override def receiveRecover: Receive = {
      case _ =>
    }
  }

}