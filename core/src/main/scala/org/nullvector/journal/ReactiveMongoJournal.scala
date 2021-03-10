package org.nullvector.journal

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import org.nullvector.UnderlyingPersistenceFactory

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.Try

class ReactiveMongoJournal(val aConfig: Config) extends AsyncWriteJournal {

  private val asyncWriteJournalOps: AsyncWriteJournalOps = UnderlyingPersistenceFactory(
    new ReactiveMongoJournalImpl(aConfig, context.system), new InMemoryAsyncWriteJournal(context.system)
  )(context.system)

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    asyncWriteJournalOps.asyncWriteMessages(messages)

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    asyncWriteJournalOps.asyncDeleteMessagesTo(persistenceId, toSequenceNr)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (recoveryCallback: PersistentRepr => Unit): Future[Unit] =
    asyncWriteJournalOps.asyncReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(recoveryCallback)

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    asyncWriteJournalOps.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr)
}
