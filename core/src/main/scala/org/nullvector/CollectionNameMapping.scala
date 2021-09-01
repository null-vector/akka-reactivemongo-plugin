package org.nullvector

import com.typesafe.config.Config

import scala.util.matching.Regex

trait CollectionNameMapping {
  def collectionNameOf(persistentId: String): Option[String]
}

class DefaultCollectionNameMapping(config: Config) extends CollectionNameMapping {
  private val separator: String =
    config.getString("akka-persistence-reactivemongo.persistence-id-separator")
  private val pattern: Regex    = buildPattern(separator.headOption)

  override def collectionNameOf(persistentId: String): Option[String] =
    persistentId match {
      case pattern(name, _) if name.isEmpty => None
      case pattern(name, _)                 => Some(name)
      case _                                => None
    }

  private def buildPattern(maybeSeparator: Option[Char]) =
    maybeSeparator match {
      case Some(char) => s"(\\w+)[$char](.+)".r
      case None       => s"()(.+)".r
    }

}
