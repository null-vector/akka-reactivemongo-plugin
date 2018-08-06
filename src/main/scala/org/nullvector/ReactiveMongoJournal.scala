package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.journal.AsyncWriteJournal
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ReactiveMongoJournal(val config: Config) extends AsyncWriteJournal with ReactiveMongoJournalImpl {

  override val actorSystem: ActorSystem = context.system

}
