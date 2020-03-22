package org.nullvector

import scala.annotation.tailrec
import scala.reflect.macros.{blackbox, whitebox}

object EventAdatpterFactory {

  def adapt[E](withManifest: String, overrideMappings: Any*): EventAdapter[E] = macro EventAdapterMacroFactory.adapt[E]

}
