package org.nullvector.journal

import akka.persistence.PersistentRepr
import akka.stream.{ActorMaterializer, Materializer}
import org.nullvector.Fields
import reactivemongo.akkastream.cursorProducer
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

trait ReactiveMongoAsyncReplay {
  this: ReactiveMongoJournalImpl =>

  implicit lazy val materializer: Materializer = ActorMaterializer()(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      collection.find(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.from_sn -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.to_sn -> BSONDocument("$lte" -> toSequenceNr)
      ), None)
        .sort(BSONDocument(Fields.to_sn -> 1))
        .cursor[BSONDocument]()
        .documentSource()
        .mapConcat(_.getAs[Seq[BSONDocument]](Fields.events).get)
        .mapAsync(Runtime.getRuntime.availableProcessors()) { doc =>
          val manifest = doc.getAs[String](Fields.manifest).get
          val rawPayload = doc.getAs[BSONDocument](Fields.payload).get
          (manifest match {
            case Fields.manifest_doc => Future.successful(rawPayload)
            case manifest => serializer.deserialize(manifest, rawPayload)
          })
            .map(payload =>
              PersistentRepr(
                payload,
                doc.getAs[Long](Fields.sequence).get,
                doc.getAs[String](Fields.persistenceId).get,
                manifest
              )
            )

        }
        .runForeach(recoveryCallback)
    }.map { _ => }
  }

}
