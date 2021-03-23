package org.nullvector

import akka.actor.typed.{ActorSystem, Extension, ExtensionId}
import com.typesafe.config.Config
import akka.actor.typed.scaladsl.adapter._
import org.nullvector.ReactiveMongoDriver.DatabaseProvider

import scala.collection.mutable

class ReactiveMongoPluginSettings(actorSystem: ActorSystem[_]) extends Extension {

  private object PathKeys {
    val persistInMemoryKey = "akka-persistence-reactivemongo.persist-in-memory"
    val databaseProviderKey = "akka-persistence-reactivemongo.database-provider"
  }

  private val valueMap: mutable.HashMap[String, Any] = mutable.HashMap()
  fillMapWith(actorSystem.settings.config)

  def persistInMemory = valueMap(PathKeys.persistInMemoryKey).asInstanceOf[Boolean]

  def databaseProvider = valueMap(PathKeys.databaseProviderKey).asInstanceOf[DatabaseProvider]

  def withDatabaseProvider(databaseProvider: DatabaseProvider) = {
    updateValues(Seq(PathKeys.databaseProviderKey -> databaseProvider))
  }

  private def updateValues(values: Seq[(String, Any)]): Unit = synchronized {
    valueMap ++= values
  }

  private def fillMapWith(config: Config): Unit = {
    updateValues(Seq(
      PathKeys.persistInMemoryKey -> config.getBoolean(PathKeys.persistInMemoryKey),
      PathKeys.databaseProviderKey -> new DefaultDatabaseProvider(actorSystem.classicSystem),
    ))
  }
}

object ReactiveMongoPluginSettings extends ExtensionId[ReactiveMongoPluginSettings] {

  override def createExtension(system: ActorSystem[_]): ReactiveMongoPluginSettings = synchronized {
    new ReactiveMongoPluginSettings(system)
  }

}
