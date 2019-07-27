package org.nullvector.journal

import java.util.Date

import akka.persistence.{AtomicWrite, PersistentRepr}
import org.nullvector.Fields
import reactivemongo.bson.{BSONDocument, BSONNull, BSONValue}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ReactiveMongoAsyncWrite {
  this: ReactiveMongoJournalImpl =>

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    for {
      collection <- rxDriver.journalCollection(messages.head.persistenceId)
      atomicDocs <- Future.traverse(messages) { atomic =>
        Future.traverse(atomic.payload) { rep =>
          serializer.serialize(rep).map {
            case (serializedRep, tags) => rep2doc(serializedRep, tags) -> tags
          }
        }.map { docs =>
          BSONDocument(
            Fields.persistenceId -> atomic.persistenceId,
            Fields.from_sn -> atomic.lowestSequenceNr,
            Fields.to_sn -> atomic.highestSequenceNr,
            Fields.events -> docs.map(_._1),
            Fields.tags -> tagsToOption(docs.map(_._2).reduce(_ ++ _)),
          )
        }
      }
      results <- Future.traverse(atomicDocs) { doc =>
        collection.insert(ordered = true).one(doc).map(result =>
          if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
        )
      }
    } yield results
  }

  def rep2doc(persistentRepr: PersistentRepr, tags: Set[String]): BSONDocument = BSONDocument(
    Fields.persistenceId -> persistentRepr.persistenceId,
    Fields.sequence -> persistentRepr.sequenceNr,
    Fields.payload -> persistentRepr.payload.asInstanceOf[BSONDocument],
    Fields.manifest -> persistentRepr.manifest,
    Fields.event_ts -> new Date(),
    Fields.tags -> tagsToOption(tags)
  )

  private def tagsToOption(tagas: Set[String]): Option[Set[String]] =
    if (tagas.isEmpty) None else Some(tagas)
}
