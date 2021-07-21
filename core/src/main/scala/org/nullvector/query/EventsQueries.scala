package org.nullvector.query

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Source}
import org.nullvector.{Fields, ReactiveMongoPlugin}
import org.nullvector.ReactiveMongoDriver.QueryType
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.{Cursor, CursorProducer}
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

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

  private def docsToEnvelop(docs: Seq[BSONDocument]): Future[Seq[EventEnvelope]] = {
    val (offsets, reps) = docs.map { doc =>
      val event = doc.getAsOpt[BSONDocument](Fields.events).get
      val offset = ObjectIdOffset(doc.getAsOpt[BSONObjectID]("_id").get)
      val payload = event.getAsOpt[BSONDocument](Fields.payload).get
      val manifest = event.getAsOpt[String](Fields.manifest).get
      val sequence = event.getAsOpt[Long](Fields.sequence).get
      val persistenceId = event.getAsOpt[String](Fields.persistenceId).get
      offset -> PersistentRepr(payload, sequence, persistenceId, manifest)
    }.unzip
    serializer.deserialize(reps)
      .map(offsets zip _)
      .map(_.map(offsetRep => EventEnvelope(
        offsetRep._1,
        offsetRep._2.persistenceId,
        offsetRep._2.sequenceNr,
        offsetRep._2.payload, offsetRep._1.id.time
      )))

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
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
      .mapConcat(identity)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .future(rxDriver.journalCollection(persistenceId))
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
      .flatMapConcat(coll => buildFindEventsByIdQuery(coll, persistenceId, fromSequenceNr, toSequenceNr))
      .via(docs2EnvelopeFlow)
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    Source
      .fromGraph(new PullerGraph[EventEnvelope, Offset](
        offset, defaultRefreshInterval, _.offset, greaterOffsetOf, offset => currentEventsByTag(tag, offset)))
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
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

  def eventsByTagQuery(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    Source.future(rxDriver.journals(collectionNames))
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
      .mapConcat(identity)
      .splitWhen(_ => true)
      .flatMapConcat(buildFindEventsByTagsQuery(_, offset, tags))
      .mergeSubstreamsWithParallelism(amountOfCores)
      .via(docs2EnvelopeFlow)
  }

  private def docs2EnvelopeFlow = Flow[BSONDocument]
    .groupedWithin(21, 1.millis)
    .mapAsync(amountOfCores)(docsToEnvelop)
    .mapConcat(identity)

  private def buildFindEventsByTagsQuery(collection: BSONCollection, offset: Offset, tags: Seq[String]) = {
    def query(field: String) = BSONDocument(field -> BSONDocument("$in" -> tags))

    import collection.AggregationFramework._

    val filterByOffsetExp = filterByOffset(offset)
    val stages: List[PipelineOperator] = List(
      Match(query(Fields.tags) ++ filterByOffsetExp),
      UnwindField(Fields.events),
      Match(query(s"${Fields.events}.${Fields.tags}")),
    )
    val hint = filterByOffsetExp match {
      case BSONDocument.empty => Some(collection.hint(BSONDocument(Fields.tags -> 1)))
      case _ => Some(collection.hint(BSONDocument("_id" -> 1, Fields.tags -> 1)))
    }

    rxDriver.explainAgg(collection)(QueryType.EventsByTag, stages, hint)

    def aggregate(implicit producer: CursorProducer[BSONDocument]): producer.ProducedCursor = {
      val aggregateCursor = collection.aggregatorContext[BSONDocument](stages, hint = hint).prepared.cursor
      producer.produce(aggregateCursor)
    }

    aggregate
      .documentSource()
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
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
      .withAttributes(ActorAttributes.dispatcher(ReactiveMongoPlugin.pluginDispatcherName))
  }

}
