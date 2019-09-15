package org.nullvector.query

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.query._
import com.typesafe.config.Config

object ReactiveMongoJournalProvider extends ExtensionId[ReactiveMongoJournalProvider] with ExtensionIdProvider {
  //val pluginId = "akka-persistence-reactivemongo.read-journal"

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoJournalProvider

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoJournalProvider = new ReactiveMongoJournalProvider(system)
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem) extends ReadJournalProvider with Extension {

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    new ReactiveMongoScalaReadJournal(system)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}



