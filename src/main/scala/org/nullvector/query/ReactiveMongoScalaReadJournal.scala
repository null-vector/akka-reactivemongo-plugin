package org.nullvector.query

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import org.nullvector.{Fields, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.reactivestreams.Subscriber
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}
import reactivemongo.akkastream.cursorProducer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class ReactiveMongoScalaReadJournal(system: ExtendedActorSystem, config: Config)
  extends akka.persistence.query.scaladsl.ReadJournal
    with akka.persistence.query.scaladsl.EventsByTagQuery
    with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.PersistenceIdsQuery
    with akka.persistence.query.scaladsl.CurrentEventsByTagQuery
    with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery {

  protected lazy val serializer = ReactiveMongoEventSerializer(system)
  protected lazy val rxDriver = ReactiveMongoDriver(system)
  protected implicit lazy val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-journal-dispatcher")
  protected implicit lazy val materializer: Materializer = ActorMaterializer()(system)

  private val refreshInterval: FiniteDuration = config.getDuration("refresh-interval", TimeUnit.MILLISECONDS).millis

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(
                                             persistenceId: String,
                                             fromSequenceNr: Long,
                                             toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source.fromFuture(rxDriver.journalCollection(persistenceId))
      .flatMapConcat(coll => buildFindEventsByIdQuery(coll, persistenceId, fromSequenceNr, toSequenceNr))
      .via(document2Envelope)
  }

  override def persistenceIds(): Source[String, NotUsed] = ???

  override def currentPersistenceIds(): Source[String, NotUsed] = ???

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
      .map(coll => buildFindEventsByTagsQuery(coll, offset, tags))
      .flatMapConcat(identity)
      .mergeSubstreamsWithParallelism(100)
      .via(document2Envelope)
  }

  def document2Envelope: Flow[BSONDocument, EventEnvelope, NotUsed] = {
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
        Fields.sequence -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.sequence -> BSONDocument("$lte" -> toSequenceNr)
      ), None)
      .sort(BSONDocument(Fields.sequence -> 1))
      .cursor[BSONDocument]()
      .documentSource()
  }


  private def filterByOffset(offset: Offset) = {
    val offsetQuery = offset match {
      case ObjectIdOffset(bsonObjectId) => BSONDocument("_id" -> BSONDocument("$gt" -> bsonObjectId))
      case NoOffset | _ => BSONDocument.empty
    }
    offsetQuery
  }
}
