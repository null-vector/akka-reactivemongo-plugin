package org.nullvector.journal.journal

import java.util.Date

import akka.persistence.AtomicWrite
import org.nullvector.journal.Fields
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ReactiveMongoAsyncWrite {
  this: ReactiveMongoJournalImpl =>

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    rxDriver.journalCollection(messages.head.persistenceId).flatMap { collection =>
      Future.traverse(messages.flatMap(_.payload))(persistentRepr =>
        serializer.serialize(persistentRepr.payload).map(serialized =>
          BSONDocument(
            Fields.persistenceId -> persistentRepr.persistenceId,
            Fields.sequence -> persistentRepr.sequenceNr,
            Fields.event -> serialized.doc,
            Fields.manifest -> serialized.manifest,
            Fields.datetime -> new Date()
          ) ++ (if (serialized.tags.nonEmpty) BSONDocument(Fields.tags -> serialized.tags) else BSONDocument.empty)
        ).flatMap(doc => collection.insert(doc)).map(result =>
          if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
        )
      )
    }
  }
}
