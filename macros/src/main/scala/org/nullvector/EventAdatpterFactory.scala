package org.nullvector

object EventAdatpterFactory {

  def adapt[E](withManifest: String): EventAdapter[E] = macro EventAdapterMacroFactory.adapt[E]

  def adapt[E](withManifest: String, tags: Any => Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithPayload2Tags[E]

  def adapt[E](withManifest: String, tags: Set[String]): EventAdapter[E] = macro EventAdapterMacroFactory.adaptWithTags[E]

}
