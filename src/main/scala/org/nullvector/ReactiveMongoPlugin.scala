package org.nullvector

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.ExecutionContext

trait ReactiveMongoPlugin {

  type Seq[+E] = immutable.Seq[E]

  val config: Config
  val actorSystem: ActorSystem

  protected lazy val serializer = ReactiveMongoEventSerializer(actorSystem)
  protected lazy val rxDriver = ReactiveMongoDriver(actorSystem)
  protected implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatcher

}
