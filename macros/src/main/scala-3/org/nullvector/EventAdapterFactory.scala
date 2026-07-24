package org.nullvector

import reactivemongo.api.bson.{BSONDocument, BSONReader, BSONWriter}

import scala.reflect.ClassTag

object EventAdapterFactory {

  inline def adapt[E](inline withManifest: String): EventAdapter[E] =
    ${ EventAdapterMacroFactory.adaptImpl[E]('withManifest) }

  inline def mappingOf[T]: BSONDocumentMapping[T] =
    ${ EventAdapterMacroFactory.mappingOfImpl[T] }

  inline def mappingOf[T](inline beforeRead: BSONDocument => BSONDocument): BSONDocumentMapping[T] =
    ${ EventAdapterMacroFactory.mappingOfWithBeforeReadImpl[T]('beforeRead) }

  inline def adapt[E](inline withManifest: String, inline tags: E => Set[String]): EventAdapter[E] =
    ${ EventAdapterMacroFactory.adaptWithPayload2TagsImpl[E]('withManifest, 'tags) }

  inline def adapt[E](inline withManifest: String, inline tags: Set[String]): EventAdapter[E] =
    ${ EventAdapterMacroFactory.adaptWithTagsImpl[E]('withManifest, 'tags) }

  inline def enumMappingOf[E]: BSONReader[E] & BSONWriter[E] =
    ${ EnumMacroFactory.enumMappingOfImpl[E] }

  inline def valueMappingOf[V <: AnyVal]: BSONReader[V] & BSONWriter[V] =
    ${ ValueClassMacroFactory.valueMappingOfImpl[V] }
}
