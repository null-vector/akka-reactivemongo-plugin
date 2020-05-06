package org.nullvector

import reactivemongo.api.bson.{BSONDocument, BSONReader, BSONWriter}

object EventAdapterFactory {

  def adapt[E](withManifest: String): EventAdapter[E] = macro EventAdapterMacroFactory.adapt[E]

  def mappingOf[T]: BSONDocumentMapping[T] = macro EventAdapterMacroFactory.mappingOf[T]

  def mappingOf[T](beforeRead: BSONDocument => BSONDocument): BSONDocumentMapping[T] = macro EventAdapterMacroFactory.mappingOfWithBeforeRead[T]

  def adapt[E](withManifest: String, tags: Any => Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithPayload2Tags[E]

  def adapt[E](withManifest: String, tags: Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithTags[E]

  def enumMappingOf[E]: BSONReader[E] with BSONWriter[E] = macro EnumMacroFactory.enumMappingOf[E]

  def valueMappingOf[V <: AnyVal]: BSONReader[V] with BSONWriter[V] = macro ValueClassMacroFactory.valueMappingOf[V]
}
