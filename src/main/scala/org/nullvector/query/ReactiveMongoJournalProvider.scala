package org.nullvector.query

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.PersistentRepr
import akka.persistence.query._
import akka.stream.{ActorMaterializer, Materializer, javadsl}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.nullvector.{Fields, ReactiveMongoDriver, ReactiveMongoEventSerializer}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}

object ReactiveMongoJournalProvider {
  val pluginId = "akka-persistence-reactivemongo.read-journal"
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    new ReactiveMongoScalaReadJournal(system, config)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}

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

  /**
    * You can use `NoOffset` to retrieve all events with a given tag or retrieve a subset of all
    * events by specifying a `Sequence` `offset`. The `offset` corresponds to an ordered sequence number for
    * the specific tag. Note that the corresponding offset of each event is provided in the
    * [[akka.persistence.query.EventEnvelope]], which makes it possible to resume the
    * stream at a later point from a given offset.
    *
    * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
    * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
    * as the `offset` parameter in a subsequent query.
    */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???

  //    offset match {
  //    case Sequence(offsetValue) ⇒
  //      val props = MyEventsByTagPublisher.props(tag, offsetValue, refreshInterval)
  //      Source.actorPublisher[EventEnvelope](props)
  //        .mapMaterializedValue(_ ⇒ NotUsed)
  //    case NoOffset ⇒ eventsByTag(tag, Sequence(0L)) //recursive
  //    case _ ⇒
  //      throw new IllegalArgumentException("LevelDB does not support " + offset.getClass.getName + " offsets")
  //  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    // implement in a similar way as eventsByTag
    ???
  }

  override def persistenceIds(): Source[String, NotUsed] = {
    // implement in a similar way as eventsByTag
    ???
  }

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    // implement in a similar way as eventsByTag
    ???
  }

  // possibility to add more plugin specific queries

  //  def byTagsWithMeta(tags: Set[String]): Source[RichEvent, QueryMetadata] = {
  //    // implement in a similar way as eventsByTag
  //    ???
  //  }
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    Source.fromFuture(rxDriver.journals())
      .mapConcat(identity)
      .map(coll => buildQueryFrom(coll, offset, tag))
      .flatMapConcat(identity)
      .mapAsync(15) { doc =>
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

  private def buildQueryFrom(collection: BSONCollection, offset: Offset, tag: String) = {
    val offsetQuery = offset match {
      case ObjectIdOffset(bsonObjectId) => BSONDocument("_id" -> BSONDocument("$gte" -> bsonObjectId))
      case NoOffset | _ => BSONDocument.empty
    }
    collection
      .find(BSONDocument(Fields.tags -> BSONDocument("$all" -> Seq(tag))) ++ offsetQuery, None)
      .sort(BSONDocument(Fields.sequence -> 1))
      .cursor[BSONDocument]()
      .documentSource()
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???
}

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

  // possibility to add more plugin specific queries
  //  def byTagsWithMeta(
  //                      tags: java.util.Set[String]): javadsl.Source[RichEvent, QueryMetadata] = {
  //    import scala.collection.JavaConverters._
  //    scaladslReadJournal.byTagsWithMeta(tags.asScala.toSet).asJava
  //  }

  override def currentEventsByTag(tag: String, offset: Offset): javadsl.Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): javadsl.Source[EventEnvelope, NotUsed] = ???
}