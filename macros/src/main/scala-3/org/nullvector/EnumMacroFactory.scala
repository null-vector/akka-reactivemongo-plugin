package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONString, BSONValue, BSONWriter}

import scala.quoted.*

private object EnumMacroFactory {

  def enumMappingOfImpl[E: Type](using Quotes): Expr[BSONReader[E] & BSONWriter[E]] = {
    import quotes.reflect.*

    val enumTypeRepr = TypeRepr.of[E].dealias
    if (!isEnumerationValue(enumTypeRepr))
      report.errorAndAbort(s"Type ${enumTypeRepr.show} does not belong to Enumeration")

    val enumTerm = enumerationInstance(enumTypeRepr)

    '{
      new BSONReader[E] with BSONWriter[E] {
        override def readTry(bson: BSONValue): scala.util.Try[E] =
          bson.asTry[String].map(s => $enumTerm.withName(s).asInstanceOf[E])

        override def writeTry(v: E): scala.util.Try[BSONValue] =
          scala.util.Try(BSONString(v.toString))
      }: BSONReader[E] & BSONWriter[E]
    }
  }

  /** Same rule as Scala 2: owner type is exactly `Enumeration`. */
  def isEnumerationValue(using Quotes)(tpe: quotes.reflect.TypeRepr): Boolean = {
    import quotes.reflect.*
    val enumeration = TypeRepr.of[Enumeration]
    val owner       = tpe.dealias.typeSymbol.owner
    scala.util
      .Try(owner.typeRef.dealias =:= enumeration)
      .orElse(scala.util.Try(owner.typeRef.dealias <:< enumeration && owner == enumeration.typeSymbol))
      .getOrElse(false)
  }

  /**
    * Path-dependent `Money.Value` has owner `Enumeration`; the actual enum object is the type prefix
    * (`Money`), matching Scala 2's `atype.toString.dropRight(1)` approach.
    */
  private def enumerationInstance(using Quotes)(tpe: quotes.reflect.TypeRepr): Expr[Enumeration] = {
    import quotes.reflect.*

    def fromPrefix(prefix: TypeRepr): Option[Expr[Enumeration]] =
      scala.util
        .Try {
          val sym =
            if (prefix.termSymbol != Symbol.noSymbol) prefix.termSymbol
            else prefix.typeSymbol
          val module =
            if (sym.flags.is(Flags.Module)) sym
            else sym.companionModule
          Ref(module).asExprOf[Enumeration]
        }
        .toOption

    tpe.dealias match {
      case TypeRef(prefix, _) =>
        fromPrefix(prefix).getOrElse {
          report.errorAndAbort(s"Cannot resolve Enumeration instance for ${tpe.show}")
        }
      case TermRef(prefix, _) =>
        fromPrefix(prefix).getOrElse {
          report.errorAndAbort(s"Cannot resolve Enumeration instance for ${tpe.show}")
        }
      case other =>
        // Fallback: drop last segment of the shown name (Scala 2 behavior).
        val normalized = other.show.replace(".package$.", ".").replace(".package.", ".").split('.').toList
        if (normalized.length < 2)
          report.errorAndAbort(s"Cannot resolve Enumeration instance for ${tpe.show}")
        else {
          val enumName = normalized.dropRight(1).mkString(".")
          scala.util
            .Try(Ref(Symbol.requiredModule(enumName)).asExprOf[Enumeration])
            .getOrElse(report.errorAndAbort(s"Cannot resolve Enumeration module '$enumName'"))
        }
    }
  }
}
