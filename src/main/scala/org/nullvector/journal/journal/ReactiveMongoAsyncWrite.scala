package org.nullvector.journal.journal

import java.util.Date

import akka.persistence.{AtomicWrite, PersistentRepr}
import org.nullvector.journal.Fields
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ReactiveMongoAsyncWrite {
  this: ReactiveMongoJournalImpl =>

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    rxDriver.journalCollection(messages.head.persistenceId).flatMap { collection =>
      Future.traverse(messages.flatMap(_.payload))(persistentRepr =>
        serializer.serialize(persistentRepr).map {
          case (serializedRep, tags) => rep2doc(serializedRep, tags)
        }.flatMap(doc => collection.insert(doc)).map(result =>
          if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
        )
      )
    }
  }

  def rep2doc(persistentRepr: PersistentRepr, tags: Set[String] = Set.empty): BSONDocument = BSONDocument(
    Fields.persistenceId -> persistentRepr.persistenceId,
    Fields.sequence -> persistentRepr.sequenceNr,
    Fields.event -> persistentRepr.payload.asInstanceOf[BSONDocument],
    Fields.manifest -> persistentRepr.manifest,
    Fields.datetime -> new Date()
  ) ++ (if (tags.nonEmpty) BSONDocument(Fields.tags -> tags) else BSONDocument.empty)
}
