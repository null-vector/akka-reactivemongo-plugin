package org.nullvector.journal

import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import org.nullvector.Fields
import reactivemongo.api.Cursor
import reactivemongo.bson.BSONDocument
import reactivemongo.akkastream.cursorProducer

import scala.concurrent.Future

trait ReactiveMongoAsyncReplay {
  this: ReactiveMongoJournalImpl =>

  implicit lazy val materializer: Materializer = ActorMaterializer()(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    println(s"Recovering $persistenceId")
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
        .mapAsync(15) { doc =>
          val manifest = doc.getAs[String](Fields.manifest).get
          serializer.deserialize(manifest, doc.getAs[BSONDocument](Fields.payload).get)
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
