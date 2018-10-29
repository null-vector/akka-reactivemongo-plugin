package org.nullvector

import scala.util.matching.Regex

trait CollectionNameMapping {

  def collectionNameOf(persistentId: String): Option[String]
}


class DefaultCollectionNameMapping extends CollectionNameMapping {

  import DefaultCollectionNameMapping._

  override def collectionNameOf(persistentId: String): Option[String] = persistentId match {
    case defaultPatter(name, _) => Some(name)
    case _ => None
  }

}

object DefaultCollectionNameMapping {
  val defaultPatter: Regex = "(\\w+)-(\\w+)".r
}

