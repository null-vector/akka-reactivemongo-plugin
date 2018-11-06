package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.{Flow, Source}
import org.nullvector.Fields
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}
import reactivemongo.akkastream.cursorProducer

trait EventsQueries
  extends akka.persistence.query.scaladsl.EventsByTagQuery
    with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.CurrentEventsByTagQuery
    with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery {

  this: ReactiveMongoScalaReadJournal =>

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long
                                    ): Source[EventEnvelope, NotUsed] = {
    Source
      .fromGraph(new EventsByIdSource(persistenceId, fromSequenceNr, toSequenceNr, refreshInterval, currentEventsByPersistenceId))
      .flatMapConcat(identity)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long
                                           ): Source[EventEnvelope, NotUsed] = {
    Source
      .fromFuture(rxDriver.journalCollection(persistenceId))
      .flatMapConcat(coll => buildFindEventsByIdQuery(coll, persistenceId, fromSequenceNr, toSequenceNr))
      .via(document2Envelope)
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    Source
      .fromGraph(new EventsByTagsSource(Seq(tag), offset, refreshInterval, currentEventsByTags))
      .flatMapConcat(identity)
  }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    currentEventsByTags(Seq(tag), offset)
  }


  def currentEventsByTags(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    Source.fromFuture(rxDriver.journals())
      .mapConcat(identity)
      .groupBy(100, _.name)
      .flatMapConcat(coll => buildFindEventsByTagsQuery(coll, offset, tags))
      .mergeSubstreamsWithParallelism(100)
      .via(document2Envelope)
  }

  private def document2Envelope: Flow[BSONDocument, EventEnvelope, NotUsed] = {
    Flow[BSONDocument].mapAsync(15) { doc =>
      val manifest = doc.getAs[String](Fields.manifest).get
      serializer.deserialize(manifest, doc.getAs[BSONDocument](Fields.event).get)
        .map(payload =>
          EventEnvelope(
            ObjectIdOffset(doc.getAs[BSONObjectID]("_id").get),
            doc.getAs[String](Fields.persistenceId).get,
            doc.getAs[Long](Fields.sequence).get,
            payload,
          )
        )
    }
  }

  private def buildFindEventsByTagsQuery(collection: BSONCollection, offset: Offset, tags: Seq[String]) = {
    collection
      .find(BSONDocument(Fields.tags -> BSONDocument("$all" -> tags)) ++ filterByOffset(offset), None)
      .sort(BSONDocument("_id" -> 1, Fields.sequence -> 1))
      .cursor[BSONDocument]()
      .documentSource()
  }

  private def buildFindEventsByIdQuery(collection: BSONCollection, persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) = {
    collection
      .find(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.sequence -> BSONDocument("$gt" -> fromSequenceNr),
        Fields.sequence -> BSONDocument("$lte" -> toSequenceNr)
      ), None)
      .sort(BSONDocument(Fields.sequence -> 1))
      .cursor[BSONDocument]()
      .documentSource()
  }

}
