package org.nullvector

import akka.actor.ActorSystem

object UnderlyingPersistenceFactory {

  def apply[T](persistInMongo: => T, persistInMemory: => T)(implicit system: ActorSystem): T = {
    val mustPersistInMemory = system.settings.config.getBoolean("akka-persistence-reactivemongo.persist-in-memory")
    if (mustPersistInMemory) persistInMemory else persistInMongo
  }

}
