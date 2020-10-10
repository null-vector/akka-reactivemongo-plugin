package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.scaladsl.Source

trait CustomReadOps {

  /*
    * Same as  [[EventsQueries#currentEventsByTag]] but events aren't serialized, instead
    * the `EventEnvelope` will contain the raw `BSONDocument`
   */
  def currentRawEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed]

  def currentRawEventsByTag(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed]

  def currentEventsByTags(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed]
}
