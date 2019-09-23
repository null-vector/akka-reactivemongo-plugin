package org.nullvector.journal

import org.nullvector._
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ReactiveMongoAsyncDeleteMessages {
  this: ReactiveMongoJournalImpl =>

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.to_sn -> BSONDocument("$lte" -> toSequenceNr),
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)): Future[Try[Unit]])
    }.transform(_.flatMap(identity))
  }
}
