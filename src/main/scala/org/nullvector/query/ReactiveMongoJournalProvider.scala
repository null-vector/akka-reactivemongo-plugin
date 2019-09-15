package org.nullvector.query

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.query._

object ReactiveMongoJournalProvider extends ExtensionId[ReactiveMongoJournalProvider] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoJournalProvider

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoJournalProvider = new ReactiveMongoJournalProvider(system)
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem) extends ReadJournalProvider with Extension {

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    new ReactiveMongoScalaReadJournal(system)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}



