package org.nullvector.query

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, Sequence, TimeBasedUUID}
import akka.stream.scaladsl.Source
import org.nullvector.{PersistInMemory, ReactiveMongoEventSerializer}
import org.nullvector.PersistInMemory.EventEntry

import scala.concurrent.{ExecutionContextExecutor, Future}

class FromMemoryReadJournal(actorSystem: ActorSystem[_]) extends ReactiveMongoScalaReadJournal {
  private implicit val ec: ExecutionContextExecutor = actorSystem.executionContext
  private val memory: PersistInMemory = PersistInMemory(actorSystem)
  private val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(actorSystem)

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def persistenceIds(): Source[String, NotUsed] = ???

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    currentEventsByTags(Seq(tag), offset)

  override def currentRawEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???

  override def currentRawEventsByTag(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByTags(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    Source
      .futureSource(memory.allEvents())
      .filter(event => offset match {
        case offset: ObjectIdOffset => event._2.offset.exists(_ > offset)
        case _ => true
      })
      .filter(_._2.tags.exists(tags.contains))
      .mapAsync(15)(event => toEventEnvelope _ tupled event)
      .mapMaterializedValue(_ => NotUsed)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def currentPersistenceIds(): Source[String, NotUsed] = ???


  def toEventEnvelope(persistenceId: String, event: EventEntry): Future[EventEnvelope] = {
    serializer
      .deserialize(event.manifest, event.event)
      .map { deserialized =>
        val offset = event.offset.get
        EventEnvelope(offset, persistenceId, event.sequence, deserialized, offset.bsonObjectId.time)
      }
  }
}
