package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson.BSONDocument

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

trait ReactiveMongoJournalImpl {

  type Seq[+E] = immutable.Seq[E]

  val config: Config

  val actorSystem: ActorSystem

  implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

  private lazy val database: DefaultDB = {
    val parsedURI = MongoConnection.parseURI("mongodb://localhost/test") match {
      case Success(_parsedURI) => _parsedURI
      case Failure(exception) => throw exception
    }
    val databaseName = parsedURI.db.getOrElse(throw new Exception("Missing database name"))
    Await.result(MongoDriver(config).connection(parsedURI).database(databaseName), 15.seconds)
  }

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val futures = messages
      .flatMap(_.payload)
      .map(persistentRepr =>
        database.collection[BSONCollection](collectionNameFrom(persistentRepr.persistenceId))
          .insert(BSONDocument(
            "persistence_id" -> persistentRepr.persistenceId,
            "event" -> persistentRepr.payload.toString
          ))
      )

    Future.sequence(futures).map(results =>
      results.map(result =>
        if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
      )
    )
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = ???

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = ???

  private def collectionNameFrom(persistenceId: String): String = persistenceId.split("-")(0)
}
