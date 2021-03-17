package org.nullvector.journal

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.nullvector.{ReactiveMongoDriver, ReactiveMongoPlugin}
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

import scala.util.Try

class ReactiveMongoJournalImpl(val config: Config, val actorSystem: ActorSystem) extends ReactiveMongoPlugin
  with AsyncWriteJournalOps
  with ReactiveMongoAsyncWrite
  with ReactiveMongoAsyncReplay
  with ReactiveMongoHighestSequence
  with ReactiveMongoAsyncDeleteMessages {

  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(actorSystem)

}
