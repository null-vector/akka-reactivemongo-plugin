package org.nullvector.crud

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.state._

class ReactiveMongoCrudProvider(system: ExtendedActorSystem) extends DurableStateStoreProvider {

  override def scaladslDurableStateStore(): scaladsl.DurableStateStore[Any]  = new ReactiveMongoCrud(system.toTyped)
  override def javadslDurableStateStore(): javadsl.DurableStateStore[AnyRef] = null
}
