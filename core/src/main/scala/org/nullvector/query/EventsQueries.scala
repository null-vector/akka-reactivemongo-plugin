package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.{Flow, Source}
import org.nullvector.Fields
import org.nullvector.ReactiveMongoDriver.QueryType
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.{Cursor, CursorProducer}
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future

trait EventsQueries
  extends akka.persistence.query.scaladsl.EventsByTagQuery
    with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.CurrentEventsByTagQuery
    with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery with CustomReadOps {

  this: ReactiveMongoScalaReadJournalImpl =>

  private val amountOfCores: Int = Runtime.getRuntime.availableProcessors()

  val greaterOffsetOf: (Offset, Offset) => Offset = (leftOffset: Offset, rightOffset: Offset) => {
    (leftOffset, rightOffset) match {
      case (NoOffset, _) => rightOffset
      case (leftId: ObjectIdOffset, rightId: ObjectIdOffset) if leftId < rightId => rightOffset
      case _ => leftOffset
    }
  }

  implicit val manifestBasedSerialization: (BSONDocument, BSONDocument) => Future[Any] = (event: BSONDocument, rawPayload: BSONDocument) => {
    val manifest = event.getAsOpt[String](Fields.manifest).get
    serializer.deserialize(manifest, rawPayload)
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .fromGraph(new PullerGraph[EventEnvelope, (Long, Long)](
        (fromSequenceNr, toSequenceNr),
        defaultRefreshInterval,
        envelope => (envelope.sequenceNr, Long.MaxValue),
        (_, fromToSequences) => fromToSequences,
        offset => currentEventsByPersistenceId(persistenceId, offset._1, offset._2)
      ))
      .mapConcat(identity)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .future(rxDriver.journalCollection(persistenceId))
      .flatMapConcat(coll => buildFindEventsByIdQuery(coll, persistenceId, fromSequenceNr, toSequenceNr))
      .via(document2Envelope(manifestBasedSerialization))
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    Source
      .fromGraph(new PullerGraph[EventEnvelope, Offset](
        offset, defaultRefreshInterval, _.offset, greaterOffsetOf, offset => currentEventsByTag(tag, offset)))
      .mapConcat(identity)

  /*
    * Query events that have a specific tag. Those events matching target tags would
    * be serialized depending on Document `manifest` field and events serializer that are provided.
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    currentEventsByTags(Seq(tag), offset)
  }

  /*
    * Same as  [[EventsQueries#currentEventsByTag]] but events aren't serialized, instead
    * the `EventEnvelope` will contain the raw `BSONDocument`
   */
  override def currentRawEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    currentRawEventsByTag(Seq(tag), offset)
  }

  override def currentRawEventsByTag(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    implicit val raw: (BSONDocument, BSONDocument) => Future[BSONDocument] = (_, rawPayload) => Future(rawPayload)
    eventsByTagQuery(tags, offset)
  }

  override def currentEventsByTags(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    eventsByTagQuery(tags, offset)
  }

  private def eventsByTagQuery(tags: Seq[String], offset: Offset)(implicit serializableMethod: (BSONDocument, BSONDocument) => Future[Any]): Source[EventEnvelope, NotUsed] = {
    Source.future(rxDriver.journals())
      .mapConcat(identity)
      .splitWhen(_ => true)
      .flatMapConcat(buildFindEventsByTagsQuery(_, offset, tags))
      .mergeSubstreamsWithParallelism(amountOfCores)
      .via(document2Envelope(serializableMethod))
  }

  private def document2Envelope(serializationMethod: (BSONDocument, BSONDocument) => Future[Any]) = Flow[BSONDocument]
    .mapAsync(amountOfCores) { doc =>
      val event: BSONDocument = doc.getAsOpt[BSONDocument](Fields.events).get
      val rawPayload: BSONDocument = event.getAsOpt[BSONDocument](Fields.payload).get
      serializationMethod(event, rawPayload)
        .map(payload => {
          val offset = ObjectIdOffset(doc.getAsOpt[BSONObjectID]("_id").get)
          EventEnvelope(
            offset,
            event.getAsOpt[String](Fields.persistenceId).get,
            event.getAsOpt[Long](Fields.sequence).get,
            payload,
            offset.id.time
          )
        })
    }

  private def buildFindEventsByTagsQuery(collection: BSONCollection, offset: Offset, tags: Seq[String]) = {
    def query(field: String) = BSONDocument(field -> BSONDocument("$in" -> tags))

    import collection.AggregationFramework._

    val stages: List[PipelineOperator] = List(
      Match(query(Fields.tags) ++ filterByOffset(offset)),
      UnwindField(Fields.events),
      Match(query(s"${Fields.events}.${Fields.tags}")),
    )
    val hint = Some(collection.hint(BSONDocument("_id" -> 1, Fields.tags -> 1)))
    rxDriver.explainAgg(collection)(QueryType.EventsByTag, stages, hint)

    def aggregate(implicit producer: CursorProducer[BSONDocument]): producer.ProducedCursor = {
      val aggregateCursor = collection.aggregatorContext[BSONDocument](stages, hint = hint).prepared.cursor
      producer.produce(aggregateCursor)
    }

    aggregate.documentSource()
  }

  private def buildFindEventsByIdQuery(coll: collection.BSONCollection, persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) = {
    coll
      .aggregateWith[BSONDocument]()(framework => List(
        framework.Match(BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.from_sn -> BSONDocument("$gt" -> fromSequenceNr),
          Fields.to_sn -> BSONDocument("$lte" -> toSequenceNr)
        )),
        framework.UnwindField(Fields.events),
        framework.Sort(framework.Ascending(s"${Fields.events}.${Fields.sequence}"))
      ))
      .documentSource()
  }

}
