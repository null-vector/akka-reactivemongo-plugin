package org.nullvector.journal.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import com.typesafe.config.Config

class ReactiveMongoJournal(val config: Config) extends AsyncWriteJournal with ReactiveMongoJournalImpl {

  lazy val actorSystem: ActorSystem = context.system

}

object ReactiveMongoJournal {
  val pluginId = "akka-persistence-reactivemongo-journal"
}
