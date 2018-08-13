package org.nullvector.journal.journal

import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import reactivemongo.api.Cursor
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

trait ReactiveMongoAsyncReplay {
  this: ReactiveMongoJournalImpl =>

  implicit lazy val materializer: Materializer = ActorMaterializer()(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] =
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      collection.find(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.sequence -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.sequence -> BSONDocument("$lte" -> toSequenceNr)
      ), None)
        .sort(BSONDocument(Fields.sequence -> 1))
        .cursor[BSONDocument]()
        .collect[List](max.toInt, Cursor.FailOnError[List[BSONDocument]]())
    }.flatMap { docs =>
      Source(docs)
        .mapAsync(15) { doc =>
          serializer.deserialize(doc.getAs[String](Fields.manifest).get, doc.getAs[BSONDocument](Fields.event).get)
            .map(payload =>
              PersistentRepr(
                payload,
                doc.getAs[Long](Fields.sequence).get,
                doc.getAs[String](Fields.persistenceId).get,
                doc.getAs[String](Fields.manifest).get
              )
            )
        }
        .runForeach(recoveryCallback)
    }.map { _ => }

}
