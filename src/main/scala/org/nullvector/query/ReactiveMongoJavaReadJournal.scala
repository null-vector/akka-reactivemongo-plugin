package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, Offset, Sequence}
import akka.stream.javadsl

class ReactiveMongoJavaReadJournal(scaladslReadJournal: ReactiveMongoScalaReadJournal)
  extends akka.persistence.query.javadsl.ReadJournal
    with akka.persistence.query.javadsl.EventsByTagQuery
    with akka.persistence.query.javadsl.EventsByPersistenceIdQuery
    with akka.persistence.query.javadsl.PersistenceIdsQuery
    with akka.persistence.query.javadsl.CurrentEventsByTagQuery
    with akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery
    with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery {

  override def eventsByTag(tag: String, offset: Offset = Sequence(0L)): javadsl.Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.eventsByTag(tag, offset).asJava

  override def eventsByPersistenceId(
                                      persistenceId: String,
                                      fromSequenceNr: Long = 0L,
                                      toSequenceNr: Long = Long.MaxValue): javadsl.Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def persistenceIds(): javadsl.Source[String, NotUsed] = scaladslReadJournal.persistenceIds().asJava

  override def currentPersistenceIds(): javadsl.Source[String, NotUsed] = scaladslReadJournal.currentPersistenceIds().asJava

  override def currentEventsByTag(tag: String, offset: Offset): javadsl.Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): javadsl.Source[EventEnvelope, NotUsed] = ???
}
