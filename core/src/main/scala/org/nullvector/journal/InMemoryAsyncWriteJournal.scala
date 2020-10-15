package org.nullvector.journal

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.nullvector.PersistInMemory.EventEntry
import org.nullvector.{PersistInMemory, ReactiveMongoEventSerializer}
import reactivemongo.api.bson.BSONDocument

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Success, Try}

class InMemoryAsyncWriteJournal(val system: ActorSystem) extends AsyncWriteJournalOps {

  import akka.actor.typed.scaladsl.adapter._

  private implicit val ec = system.dispatcher
  private implicit val materializer: Materializer = Materializer.matFromSystem(system)
  private val eventSerializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(system)
  private val persistInMemory: PersistInMemory = PersistInMemory(system.toTyped)

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    Source(messages).mapAsync(1)(atomic =>
      Source(atomic.payload)
        .mapAsync(1)(eventSerializer.serialize)
        .map(slized => persistentRepr2EventEntry _ tupled slized)
        .runWith(Sink.seq)
        .flatMap(events => persistInMemory.addEvents(atomic.persistenceId, events).transform(tried => Try(tried.map(_ => ()))))
    )
      .runWith(Sink.seq)
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    persistInMemory.removeEventsTo(persistenceId, toSequenceNr)
    Future.successful(())
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    Source.future(persistInMemory.eventsOf(persistenceId))
      .mapConcat(identity)
      .map(_.eventEntry)
      .filter(entry => entry.sequence >= fromSequenceNr && entry.sequence <= toSequenceNr)
      .mapAsync(15)(entry => eventSerializer.deserialize(entry.manifest, entry.event).map(payload => entry -> payload))
      .map(entryAndPayload => eventEntry2PersistentRepr(persistenceId) _ tupled entryAndPayload)
      .runForeach(recoveryCallback)
      .map(_ => ())
  }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    persistInMemory.highestSequenceOf(persistenceId)
  }

  def persistentRepr2EventEntry(rep: PersistentRepr, tags: Set[String]): EventEntry = {
    EventEntry(rep.persistenceId, rep.sequenceNr, rep.manifest, rep.payload.asInstanceOf[BSONDocument], tags)
  }

  def eventEntry2PersistentRepr(persistenceId: String)(entry: EventEntry, payload: Any): PersistentRepr = {
    PersistentRepr(payload, entry.sequence, persistenceId, entry.manifest)
  }

}
