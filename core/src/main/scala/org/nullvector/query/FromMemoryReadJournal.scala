package org.nullvector.query

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.nullvector.PersistInMemory
import org.nullvector.PersistInMemory.EventWithOffset
import org.nullvector.typed.ReactiveMongoEventSerializer
import reactivemongo.api.bson.BSONDocument

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContextExecutor, Future}

class FromMemoryReadJournal(actorSystem: ActorSystem[_]) extends ReactiveMongoScalaReadJournal {
  private implicit val ec: ExecutionContextExecutor    =
    actorSystem.executionContext
  private implicit val mat: Materializer               =
    Materializer.matFromSystem(actorSystem)
  private val memory: PersistInMemory                  = PersistInMemory(actorSystem)
  private val serializer: ReactiveMongoEventSerializer =
    ReactiveMongoEventSerializer(actorSystem)
  val defaultRefreshInterval: FiniteDuration           = 500.millis
  val greaterOffsetOf: (Offset, Offset) => Offset      =
    (leftOffset: Offset, rightOffset: Offset) => {
      (leftOffset, rightOffset) match {
        case (NoOffset, _)                                                         => rightOffset
        case (leftId: ObjectIdOffset, rightId: ObjectIdOffset) if leftId < rightId =>
          rightOffset
        case _                                                                     => leftOffset
      }
    }

  override def eventsByTag(
      tag: String,
      offset: Offset
  ): Source[EventEnvelope, NotUsed] =
    Source
      .fromGraph(
        new PullerGraph[EventEnvelope, Offset](
          offset,
          defaultRefreshInterval,
          _.offset,
          greaterOffsetOf,
          offset => currentEventsByTag(tag, offset)
        )
      )
      .mapConcat(identity)

  override def currentEventsByTag(
      tag: String,
      offset: Offset
  ): Source[EventEnvelope, NotUsed] =
    currentEventsByTags(Seq(tag), offset)

  override def currentRawEventsByTag(
      tag: String,
      offset: Offset
  ): Source[EventEnvelope, NotUsed] = {
    currentRawEventsByTag(Seq(tag), offset)
  }

  override def currentRawEventsByTag(
      tags: Seq[String],
      offset: Offset
  ): Source[EventEnvelope, NotUsed] = {
    sourceEvents(tags, offset, keepRaw = true)
  }

  override def currentEventsByTags(
      tags: Seq[String],
      offset: Offset
  ): Source[EventEnvelope, NotUsed] = {
    sourceEvents(tags, offset, keepRaw = false)
  }

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] = {
    Source
      .futureSource(memory.eventsOf(persistenceId).map(Source(_)))
      .filter(event => event.eventEntry.sequence > fromSequenceNr && event.eventEntry.sequence <= toSequenceNr)
      .mapAsync(15)(event => toEventEnvelope(keepRaw = false)(event))
      .mapMaterializedValue(_ => NotUsed)
  }

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    Source
      .futureSource(memory.allPersistenceId())
      .mapMaterializedValue(_ => NotUsed)
  }

  def toEventEnvelope(
      keepRaw: Boolean
  )(event: EventWithOffset): Future[EventEnvelope] = {
    val entry = event.eventEntry

    def eventEnvelope(deserialized: Any, offset: ObjectIdOffset) =
      EventEnvelope(
        offset,
        entry.persistenceId,
        entry.sequence,
        deserialized,
        offset.id.time
      )

    if (keepRaw)
      Future.successful(eventEnvelope(entry.event, event.offset))
    else
      serializer
        .deserialize(
          Seq(
            PersistentRepr(
              entry.event,
              entry.sequence,
              entry.persistenceId,
              entry.manifest
            )
          )
        )
        .map(deserialized => eventEnvelope(deserialized.head.payload, event.offset))
  }

  private def sourceEvents(
      tags: Seq[String],
      offset: Offset,
      keepRaw: Boolean
  ) = {
    Source
      .futureSource(memory.allEvents(offset))
      .filter(_.eventEntry.tags.exists(tags.contains))
      .mapAsync(Runtime.getRuntime.availableProcessors())(
        toEventEnvelope(keepRaw)(_)
      )
      .mapMaterializedValue(_ => NotUsed)
  }

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] = ???

  override def persistenceIds(): Source[String, NotUsed] = ???

  /** Let a fine grained filter on event directly from Mongo database. WARNING: Using additional filter may cause a full scan collection or
    * index.
    *
    * @param tag
    *   the tagged event
    * @param offset
    *   from start reading
    * @param eventFilter
    *   a document filter for events
    * @return
    */
  override def currentEventsByTag(
      tag: String,
      offset: Offset,
      eventFilter: BSONDocument,
      filterHint: Option[BSONDocument]
  ): Source[EventEnvelope, NotUsed] = ???

  override def eventsByTags(
      tags: Seq[String],
      offset: Offset,
      eventFilter: BSONDocument,
      filterHint: Option[BSONDocument],
      refreshInterval: FiniteDuration
  ): Source[EventEnvelope, NotUsed] = ???
}
