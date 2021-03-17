package org.nullvector.journal

import akka.persistence.PersistentRepr
import akka.stream.Materializer
import org.nullvector.Fields
import org.nullvector.ReactiveMongoDriver.QueryType
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future

trait ReactiveMongoAsyncReplay {
  this: ReactiveMongoJournalImpl =>

  private implicit lazy val mat: Materializer = Materializer.matFromSystem(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection: BSONCollection =>
      val query = BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.to_sn -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.from_sn -> BSONDocument("$lte" -> toSequenceNr),
      )
      val queryBuilder = collection.find(query)
      rxDriver.explain(collection)(QueryType.Recovery, queryBuilder)
      queryBuilder
        .cursor[BSONDocument]()
        .documentSource(if (max >= Int.MaxValue) Int.MaxValue else max.intValue())
        .mapConcat(_.getAsOpt[Seq[BSONDocument]](Fields.events).get)
        .mapAsync(Runtime.getRuntime.availableProcessors()) { doc =>
          val manifest = doc.getAsOpt[String](Fields.manifest).get
          val rawPayload = doc.getAsOpt[BSONDocument](Fields.payload).get
          serializer.deserialize(manifest, rawPayload).map(payload =>
            PersistentRepr(
              payload,
              doc.getAsOpt[Long](Fields.sequence).get,
              doc.getAsOpt[String](Fields.persistenceId).get,
              manifest
            )
          )
        }
        .runForeach(recoveryCallback)
    }.map { _ => }
  }


}
