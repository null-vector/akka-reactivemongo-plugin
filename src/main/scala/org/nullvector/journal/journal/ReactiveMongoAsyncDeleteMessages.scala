package org.nullvector.journal.journal

import org.nullvector.journal.Fields
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait ReactiveMongoAsyncDeleteMessages {
  this: ReactiveMongoJournalImpl =>

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      val eventualElement = deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.sequence -> BSONDocument("$lte" -> toSequenceNr),
        ), None, None
      )
      eventualElement.map(el => deleteBuilder.many(Seq(el))).map(_ => Unit)
    }
  }
}
