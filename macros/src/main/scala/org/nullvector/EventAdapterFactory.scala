package org.nullvector

object EventAdapterFactory {

  def adapt[E](withManifest: String): EventAdapter[E] = macro EventAdapterMacroFactory.adapt[E]

  def mappingOf[T]: BSONDocumentMapping[T] = macro EventAdapterMacroFactory.mappingOf[T]

  def adapt[E](withManifest: String, tags: Any => Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithPayload2Tags[E]

  def adapt[E](withManifest: String, tags: Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithTags[E]

}
