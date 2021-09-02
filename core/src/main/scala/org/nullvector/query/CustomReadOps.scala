package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.scaladsl.Source
import reactivemongo.api.bson.BSONDocument

import scala.concurrent.duration.FiniteDuration

trait CustomReadOps {

  /** Let a fine grained filter on event directly from Mongo database. WARNING: Using additional filter may cause a full scan collection or
    * index. You probable want to add a new index and provide it in the hint parameter.
    * @param tag
    *   the tagged event
    * @param offset
    *   from start reading
    * @param eventFilter
    *   a document filter for events
    * @param filterHint
    *   an optional hint index to use with filter
    * @return
    */
  def currentEventsByTag(
      tag: String,
      offset: Offset,
      eventFilter: BSONDocument,
      filterHint: Option[BSONDocument]
  ): Source[EventEnvelope, NotUsed]

  def eventsByTags(
      tags: Seq[String],
      offset: Offset,
      eventFilter: BSONDocument,
      filterHint: Option[BSONDocument],
      refreshInterval: FiniteDuration
  ): Source[EventEnvelope, NotUsed]

  /** Same as [[EventsQueries#currentEventsByTag]] but events aren't serialized, instead the `EventEnvelope` will contain the raw
    * `BSONDocument`
    */
  def currentRawEventsByTag(
      tag: String,
      offset: Offset
  ): Source[EventEnvelope, NotUsed]

  def currentRawEventsByTag(
      tags: Seq[String],
      offset: Offset
  ): Source[EventEnvelope, NotUsed]

  def currentEventsByTags(
      tags: Seq[String],
      offset: Offset
  ): Source[EventEnvelope, NotUsed]
}
