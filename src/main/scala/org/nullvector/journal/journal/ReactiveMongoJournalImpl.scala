package org.nullvector.journal.journal

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.journal.{ReactiveMongoDriver, ReactiveMongoEventSerializer}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

trait ReactiveMongoJournalImpl extends ReactiveMongoAsyncWrite with ReactiveMongoAsyncReplay with ReactiveMongoHighestSequence {

  object Fields {
    val persistenceId = "persistence_id"
    val sequence = "sequence"
    val event = "event"
    val manifest = "manifest"
    val datetime = "datetime"
  }

  type Seq[+E] = immutable.Seq[E]

  val config: Config
  val actorSystem: ActorSystem

  protected lazy val serializer = ReactiveMongoEventSerializer(actorSystem)
  protected lazy val rxDriver = ReactiveMongoDriver(actorSystem)
  protected implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???

}
