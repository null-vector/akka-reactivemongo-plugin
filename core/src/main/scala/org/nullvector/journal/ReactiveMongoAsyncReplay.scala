package org.nullvector.journal

import akka.persistence.PersistentRepr
import akka.stream.{ActorMaterializer, Materializer}
import org.nullvector.Fields
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.bson._

import scala.concurrent.Future

trait ReactiveMongoAsyncReplay {
  this: ReactiveMongoJournalImpl =>

  private implicit lazy val mat: Materializer = Materializer.matFromSystem(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      val query = BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.from_sn -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.to_sn -> BSONDocument("$lte" -> toSequenceNr)
      )
      collection
        .find(query, Option.empty[BSONDocument])
        .sort(BSONDocument(Fields.to_sn -> 1))
        .cursor[BSONDocument]()
        .documentSource()
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
