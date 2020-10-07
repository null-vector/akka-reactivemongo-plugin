package org.nullvector.journal

import akka.persistence.{AtomicWrite, PersistentRepr}

import scala.concurrent.Future
import scala.util.Try

trait AsyncWriteJournalOps {

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]]

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit]

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit]

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]
}
