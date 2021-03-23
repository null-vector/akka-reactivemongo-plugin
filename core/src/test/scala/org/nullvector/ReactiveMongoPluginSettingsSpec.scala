package org.nullvector

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.{FlatSpec, Matchers}


class ReactiveMongoPluginSettingsSpec() extends FlatSpec with Matchers {

  val system = ActorSystem(Behaviors.empty, "Settings")

  it should " persist in memory setting " in {

    ReactiveMongoPluginSettings(system).persistInMemory shouldBe false
  }
}
