package org.nullvector.journal

trait CollectionNameMapping {

  def collectionNameOf(persistentId: String): Option[String]

}

class DefaultCollectionNameMapping extends CollectionNameMapping {

  override def collectionNameOf(persistentId: String): Option[String] = None

}

