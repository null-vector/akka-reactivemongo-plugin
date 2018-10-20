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
    for {
      collection <- rxDriver.journalCollection(messages.head.persistenceId)
      atomicDocs <- Future.traverse(messages){ atomic =>
        Future.traverse(atomic.payload){ rep =>
          serializer.serialize(rep).map {
            case (serializedRep, tags) => rep2doc(serializedRep, tags)
          }
        }
      }
      results <- Future.traverse(atomicDocs){ docs =>
        collection.insert[BSONDocument](ordered = true).many(docs).map(result =>
          if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
        )
      }
    } yield results
  }

  def rep2doc(persistentRepr: PersistentRepr, tags: Set[String]): BSONDocument = BSONDocument(
    Fields.persistenceId -> persistentRepr.persistenceId,
    Fields.sequence -> persistentRepr.sequenceNr,
    Fields.event -> persistentRepr.payload.asInstanceOf[BSONDocument],
    Fields.manifest -> persistentRepr.manifest,
    Fields.datetime -> new Date(),
    Fields.tags -> tags
  )
}
