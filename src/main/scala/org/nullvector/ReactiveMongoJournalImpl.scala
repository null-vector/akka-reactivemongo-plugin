package org.nullvector

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import reactivemongo.bson.{BSONDocument, BSONString}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait ReactiveMongoJournalImpl {

  type Seq[+E] = immutable.Seq[E]

  val config: Config

  val actorSystem: ActorSystem

  private lazy val serializer = ReactiveMongoEventSerializer(actorSystem)
  private lazy val rxDriver = ReactiveMongoDriver(actorSystem)
  implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    rxDriver.collectionFor(messages.head.persistenceId).flatMap { collection =>
      Future.traverse(messages.flatMap(_.payload))(persistentRepr =>
        serializer.serialize(persistentRepr.manifest, persistentRepr.payload).map(serialized =>
          BSONDocument(
            "persistence_id" -> persistentRepr.persistenceId,
            "sequence" -> persistentRepr.sequenceNr,
            "event" -> serialized
          )
        ).flatMap(doc => collection.insert(doc)).map(result =>
          if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
        )
      )
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] =
    Future.successful(Unit)

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    rxDriver.collectionFor(persistenceId).flatMap { collection =>
      import collection.BatchCommands.AggregationFramework._

      val $match = Match(BSONDocument("persistence_id" -> persistenceId))
      val $group = Group(BSONString("$persistence_id"))("seq" -> MaxField("sequence"))

      collection.aggregatorContext[BSONDocument]($match, List($group))
        .prepared.cursor.headOption.map(_.map(_.getAs[Long]("seq").get).getOrElse(0l))
    }
  }

}
