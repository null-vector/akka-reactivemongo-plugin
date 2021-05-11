package org.nullvector

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoPlugin.pluginDispatcherName

import scala.collection.immutable
import scala.concurrent.ExecutionContext

object ReactiveMongoPlugin {
  val pluginDispatcherName = "akka-persistence-reactivemongo-dispatcher"
}

trait ReactiveMongoPlugin {

  type Seq[+E] = immutable.Seq[E]

  val config: Config
  val actorSystem: ActorSystem

  protected lazy val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(actorSystem)
  protected implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatchers.lookup(pluginDispatcherName)

}

