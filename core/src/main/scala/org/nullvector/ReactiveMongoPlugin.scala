package org.nullvector

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoPlugin.pluginDispatcherName
import org.nullvector.typed.ReactiveMongoEventSerializer

import scala.collection.immutable
import scala.concurrent.ExecutionContext

object ReactiveMongoPlugin {
  val pluginDispatcherName = "akka-persistence-reactivemongo-dispatcher"
}

trait ReactiveMongoPlugin {
  val config: Config
  val actorSystem: ActorSystem

  protected lazy val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(actorSystem.toTyped)
  protected implicit lazy val dispatcher: ExecutionContext = actorSystem.dispatchers.lookup(pluginDispatcherName)

}

