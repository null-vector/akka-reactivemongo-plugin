package org.nullvector.journal

import akka.persistence.PersistentRepr
import org.nullvector.Fields
import org.nullvector.ReactiveMongoDriver.QueryType
import org.nullvector.logging.LoggerPerClassAware
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait ReactiveMongoAsyncReplay extends LoggerPerClassAware {
  this: ReactiveMongoJournalImpl =>

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    logger.debug(s"Recovering events for {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)

    def buildQuery(collection: BSONCollection) = {
      val query = BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.to_sn -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.from_sn -> BSONDocument("$lte" -> toSequenceNr),
      )
      val index = BSONDocument(
        Fields.persistenceId -> 1,
        Fields.to_sn -> 1,
        Fields.from_sn -> 1,
      )
      collection.find(query).hint(collection.hint(index))
    }

    def getEvent(document: BSONDocument): Seq[BSONDocument] = document.getAsOpt[Seq[BSONDocument]](Fields.events).get

    def deserializeEvents(bsonEvents: Seq[BSONDocument]): Future[Seq[PersistentRepr]] = {
      val persistenceReps = bsonEvents.map(bsonEvent =>
        PersistentRepr(
          bsonEvent.getAsOpt[BSONDocument](Fields.payload).get,
          bsonEvent.getAsOpt[Long](Fields.sequence).get,
          bsonEvent.getAsOpt[String](Fields.persistenceId).get,
          bsonEvent.getAsOpt[String](Fields.manifest).get
        )
      )
      serializer.deserializeAll(persistenceReps)
    }

    for {
      collection <- rxDriver.journalCollection(persistenceId)
      query = buildQuery(collection)
      _ = rxDriver.explain(collection)(QueryType.Recovery, query)
      documents <- query.cursor[BSONDocument]().collect[List]()
      _ = logger.debug(s"Recovered ${documents.size} for persistenceId:$persistenceId from mongo")
      bsonEvents = documents.flatMap(getEvent)
      events <- deserializeEvents(bsonEvents)
    } yield events.foreach(recoveryCallback)
  }

}
