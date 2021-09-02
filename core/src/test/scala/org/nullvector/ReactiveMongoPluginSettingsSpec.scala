package org.nullvector

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReactiveMongoPluginSettingsSpec() extends AnyFlatSpec with Matchers {

  val system = ActorSystem(Behaviors.empty, "Settings")

  it should " persist in memory setting " in {

    ReactiveMongoPluginSettings(system).persistInMemory shouldBe false
  }
}
