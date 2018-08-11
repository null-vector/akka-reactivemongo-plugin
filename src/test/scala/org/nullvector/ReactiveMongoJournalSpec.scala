package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class ReactiveMongoJournalSpec() extends TestKit(ActorSystem("ReactiveMongoPlugin")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val reactiveMongoJournalImpl = new ReactiveMongoJournalImpl {
    override val config: Config = ConfigFactory.load()
    override val actorSystem: ActorSystem = system
  }

  "ReactiveMongoJournalImpl" should {
    "asyncWriteMessages" in {

      val pId = s"SomeCollection-${Random.nextLong().abs}"

      val events = immutable.Seq(
        AtomicWrite(PersistentRepr(payload = List(1,2,3), persistenceId = pId, sequenceNr = 23)),
        AtomicWrite(PersistentRepr(payload = Some(3.14), persistenceId = pId, sequenceNr = 24)),
        AtomicWrite(PersistentRepr(payload = "OneMoreEvent", persistenceId = pId, sequenceNr = 25))
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