package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.{Flow, Source}
import org.nullvector.Fields
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}

import scala.concurrent.Future

trait EventsQueries
  extends akka.persistence.query.scaladsl.EventsByTagQuery
    with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.CurrentEventsByTagQuery
    with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery {

  this: ReactiveMongoScalaReadJournal =>

  private val amountOfCores: Int = Runtime.getRuntime.availableProcessors()

  val greaterOffsetOf: (Offset, Offset) => Offset = (leftOffset: Offset, rightOffset: Offset) => {
    (leftOffset, rightOffset) match {
      case (NoOffset, _) => rightOffset
      case (leftId: ObjectIdOffset, rightId: ObjectIdOffset) if leftId < rightId => rightOffset
      case _ => leftOffset
    }
  }

  implicit val manifestBasedSerialization: (BSONDocument, BSONDocument) => Future[Any] = (event: BSONDocument, rawPayload: BSONDocument) => event.getAs[String](Fields.manifest).get match {
    case Fields.manifest_doc => Future.successful(rawPayload)
    case manifest => serializer.deserialize(manifest, rawPayload)
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .fromGraph(new PullerGraph[EventEnvelope, (Long, Long)](
        (fromSequenceNr, toSequenceNr),
        defaultRefreshInterval,
        e => (e.sequenceNr, Long.MaxValue),
        (_, r) => r,
        offset => currentEventsByPersistenceId(persistenceId, offset._1, offset._2)
      ))
      .flatMapConcat(identity)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .future(rxDriver.journalCollection(persistenceId))
      .flatMapConcat(coll => buildFindEventsByIdQuery(coll, persistenceId, fromSequenceNr, toSequenceNr))
      .via(document2Envelope(manifestBasedSerialization))
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = Source
    .fromGraph(new PullerGraph[EventEnvelope, Offset](
      offset, defaultRefreshInterval, _.offset, greaterOffsetOf, o => currentEventsByTag(tag, o))
    )
    .flatMapConcat(identity)

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
  def currentRawEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    currentRawEventsByTag(Seq(tag), offset)
  }

  def currentRawEventsByTag(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    implicit val raw = (_: BSONDocument, rawPayload: BSONDocument) => Future(rawPayload)
    eventsByTagQuery(tags, offset)
  }

  def currentEventsByTags(tags: Seq[String], offset: Offset): Source[EventEnvelope, NotUsed] = {
    eventsByTagQuery(tags, offset)
  }

  private def eventsByTagQuery(tags: Seq[String], offset: Offset)(implicit serializableMethod: (BSONDocument, BSONDocument) => Future[Any]): Source[EventEnvelope, NotUsed] = {
    Source.lazyFuture(() => rxDriver.journals())
      .mapConcat(identity)
      .splitWhen(_ => true)
      .flatMapConcat(buildFindEventsByTagsQuery(_, offset, tags))
      .mergeSubstreams
      .via(document2Envelope(serializableMethod))
  }

  private def document2Envelope(serializationMethod: (BSONDocument, BSONDocument) => Future[Any]) = Flow[BSONDocument]
      .mapAsync(amountOfCores) { doc =>
        val event: BSONDocument = doc.getAs[BSONDocument](Fields.events).get
        val rawPayload: BSONDocument = event.getAs[BSONDocument](Fields.payload).get
        serializationMethod(event, rawPayload)
          .map(payload => EventEnvelope(
            ObjectIdOffset(doc.getAs[BSONObjectID]("_id").get),
            event.getAs[String](Fields.persistenceId).get,
            event.getAs[Long](Fields.sequence).get,
            payload,
          ))
      }

  private def buildFindEventsByTagsQuery(collection: BSONCollection, offset: Offset, tags: Seq[String]) = {

    def query(field: String) = BSONDocument(field -> BSONDocument("$in" -> tags))

    import collection.BatchCommands.AggregationFramework._
    val $1stMatch = Match(query(Fields.tags) ++ filterByOffset(offset))
    val $unwind = UnwindField(Fields.events)
    val $2ndMatch = Match(query(s"${Fields.events}.${Fields.tags}"))

    collection
      .aggregateWith[BSONDocument]()(_ => ($1stMatch, List($unwind, $2ndMatch)))
      .documentSource()
  }

  private def buildFindEventsByIdQuery(collection: BSONCollection, persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) = {
    import collection.BatchCommands.AggregationFramework._
    val $match = Match(BSONDocument(
      Fields.persistenceId -> persistenceId,
      Fields.from_sn -> BSONDocument("$gt" -> fromSequenceNr),
      Fields.to_sn -> BSONDocument("$lte" -> toSequenceNr)
    ))
    val $unwind = UnwindField(Fields.events)
    val $sort = Sort(Ascending(s"${Fields.events}.${Fields.sequence}"))

    collection
      .aggregateWith[BSONDocument]()(_ => ($match, List($unwind, $sort)))
      .documentSource()
  }

}
