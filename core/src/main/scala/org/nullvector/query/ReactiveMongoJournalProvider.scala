package org.nullvector.query

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.query._
import org.nullvector.UnderlyingPersistenceFactory
import org.nullvector.snapshot.{InMemorySnapshotStore, ReactiveMongoSnapshotImpl}

object ReactiveMongoJournalProvider extends ExtensionId[ReactiveMongoJournalProvider] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoJournalProvider

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoJournalProvider = new ReactiveMongoJournalProvider(system)
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem) extends ReadJournalProvider with Extension {

  import akka.actor.typed.scaladsl.adapter._

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal = UnderlyingPersistenceFactory(
    new ReactiveMongoScalaReadJournalImpl(system),
    new FromMemoryReadJournal(system.toTyped)
  )(system)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal = new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}



