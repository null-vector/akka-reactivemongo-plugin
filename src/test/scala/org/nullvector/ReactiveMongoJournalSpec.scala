package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.Random
import scala.concurrent.duration._

class ReactiveMongoJournalSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val reactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  "xxx" should {
    "yyy" in {

      def pId = s"Hola-${Random.nextLong().abs}"

      val events = immutable.Seq(
        AtomicWrite(PersistentRepr(payload = "SomeEvent", persistenceId = pId, sequenceNr = 1)),
        AtomicWrite(PersistentRepr(payload = "OtherEvent", persistenceId = pId, sequenceNr = 2))
      )

      val eventualTriedUnits = reactiveMongoJournalImpl.asyncWriteMessages(events)

      val triedUnits = Await.result(eventualTriedUnits, 15.seconds)
      triedUnits.foreach(println)
    }
  }

  override def afterAll {
    shutdown()
  }


}