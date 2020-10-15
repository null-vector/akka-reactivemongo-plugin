package org.nullvector.journal

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.{ReactiveMongoDriver, ReactiveMongoPlugin}

class ReactiveMongoJournalImpl(val config: Config, val actorSystem: ActorSystem) extends ReactiveMongoPlugin
  with AsyncWriteJournalOps
  with ReactiveMongoAsyncWrite
  with ReactiveMongoAsyncReplay
  with ReactiveMongoHighestSequence
  with ReactiveMongoAsyncDeleteMessages {

  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(actorSystem)

}
