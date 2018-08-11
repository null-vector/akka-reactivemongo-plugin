package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson.{BSONBinary, BSONDocument, Subtype}

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

trait ReactiveMongoJournalImpl {

  type Seq[+E] = immutable.Seq[E]

  val config: Config

  val actorSystem: ActorSystem

  private lazy val serializer = ReactiveMongoEventSerializer(actorSystem)
  private lazy val rxDriver = ReactiveMongoDriver(actorSystem)
  implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val eventualResults = messages.map(atomic =>
      atomic.persistenceId -> atomic.payload.map(persistentRepr =>
        serializer.serialize(persistentRepr.manifest, persistentRepr.payload).map(serialized =>
          BSONDocument(
            "persistence_id" -> persistentRepr.persistenceId,
            "event" -> serialized,
            "sequence" -> persistentRepr.sequenceNr
          )
        )
      )
    ).map(collNameAndDocs =>
      for {
        docs <- Future.sequence(collNameAndDocs._2)
        coll <- rxDriver.collectionFor(collNameAndDocs._1)
        results <- coll.insert(ordered = true).many(docs)
      } yield results
    )

    Future.sequence(eventualResults).map(_.map(result =>
      //TODO: Rollback inserted docs in case an error occur
      if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
    ))
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = ???

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.successful(0l)

}
