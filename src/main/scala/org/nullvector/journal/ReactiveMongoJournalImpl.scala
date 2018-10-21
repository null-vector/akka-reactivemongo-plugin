package org.nullvector.journal

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.{ReactiveMongoDriver, ReactiveMongoEventSerializer, ReactiveMongoPlugin}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

trait ReactiveMongoJournalImpl extends ReactiveMongoPlugin
  with ReactiveMongoAsyncWrite
  with ReactiveMongoAsyncReplay
  with ReactiveMongoHighestSequence
  with ReactiveMongoAsyncDeleteMessages {

}
