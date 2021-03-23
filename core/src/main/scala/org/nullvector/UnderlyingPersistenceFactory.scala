package org.nullvector

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps

object UnderlyingPersistenceFactory {

  def apply[T](persistInMongo: => T, persistInMemory: => T)(implicit system: ActorSystem): T = {
    val mustPersistInMemory = ReactiveMongoPluginSettings(system.toTyped).persistInMemory
    if (mustPersistInMemory) persistInMemory else persistInMongo
  }

}
