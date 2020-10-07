package org.nullvector.journal

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoPlugin

class ReactiveMongoJournalImpl(val config: Config, val actorSystem: ActorSystem) extends ReactiveMongoPlugin
  with AsyncWriteJournalOps
  with ReactiveMongoAsyncWrite
  with ReactiveMongoAsyncReplay
  with ReactiveMongoHighestSequence
  with ReactiveMongoAsyncDeleteMessages {

}
