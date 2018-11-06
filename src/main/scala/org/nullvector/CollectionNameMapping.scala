package org.nullvector

import scala.util.matching.Regex

trait CollectionNameMapping {

  def collectionNameOf(persistentId: String): Option[String]
}

class DefaultCollectionNameMapping extends CollectionNameMapping {

  import DefaultCollectionNameMapping._

  override def collectionNameOf(persistentId: String): Option[String] = persistentId match {
    case defaultPattern(name, _) => Some(name)
    case _ => None
  }

}

object DefaultCollectionNameMapping {
  val defaultPattern: Regex = "(\\w+)-(\\w+)".r
}

