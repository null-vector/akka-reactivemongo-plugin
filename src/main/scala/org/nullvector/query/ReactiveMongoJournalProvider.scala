package org.nullvector.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query._
import com.typesafe.config.Config

object ReactiveMongoJournalProvider {
  val pluginId = "akka-persistence-reactivemongo.read-journal"
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    new ReactiveMongoScalaReadJournal(system, config)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}



