package org.nullvector.journal.journal

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.journal.{ReactiveMongoDriver, ReactiveMongoEventSerializer}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

trait ReactiveMongoJournalImpl extends ReactiveMongoAsyncWrite with ReactiveMongoAsyncReplay with ReactiveMongoHighestSequence {

  type Seq[+E] = immutable.Seq[E]

  val config: Config
  val actorSystem: ActorSystem

  protected lazy val serializer = ReactiveMongoEventSerializer(actorSystem)
  protected lazy val rxDriver = ReactiveMongoDriver(actorSystem)
  protected implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???

}
