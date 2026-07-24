package org.nullvector

import reactivemongo.api.bson._

import scala.deriving.Mirror
import scala.quoted.*
import scala.reflect.ClassTag
import scala.util.Failure

/**
  * Lean Mirror-based [[BSONDocumentHandler]] derivation for Scala 3.
  *
  * Nested document / enum / value codecs are derived recursively when not already
  * in scope at the macro call site (so user overrides still win via [[Expr.summon]]).
  * Sealed discriminators follow [[MacroConfiguration]].
  */
private[nullvector] object BsonDocumentMappingDeriver {

  def forType[T: Type](using Quotes): Expr[BSONDocumentHandler[T]] = {
    import quotes.reflect.*

    val tpr = TypeRepr.of[T].dealias

    if (EnumMacroFactory.isEnumerationValue(tpr) || isValueClass(tpr))
      report.errorAndAbort(
        s"${tpr.show} is not a BSON document type"
      )
    else if (isSupportedTrait(tpr.typeSymbol))
      sumHandler[T]
    else if (isCaseObjectOrEmptyProduct(tpr))
      caseObjectHandler[T]
    else if (tpr.typeSymbol.isClassDef && tpr.typeSymbol.flags.is(Flags.Case))
      productHandler[T]
    else
      report.errorAndAbort(
        s"Cannot derive BSONDocumentHandler for ${tpr.show}; expected case class or sealed trait"
      )
  }

  private def macroConfig(using Quotes): Expr[MacroConfiguration] =
    Expr.summon[MacroConfiguration].getOrElse('{ MacroConfiguration() })

  private def isValueClass(using Quotes)(tpe: quotes.reflect.TypeRepr): Boolean = {
    import quotes.reflect.*
    tpe <:< TypeRepr.of[AnyVal] && !tpe.typeSymbol.flags.is(Flags.Trait)
  }

  private def isSupportedTrait(using Quotes)(sym: quotes.reflect.Symbol): Boolean = {
    import quotes.reflect.*
    sym.isClassDef &&
    sym.flags.is(Flags.Trait) &&
    sym.flags.is(Flags.Sealed) &&
    !sym.fullName.startsWith("scala")
  }

  private def isCaseObjectOrEmptyProduct(using Quotes)(tpr: quotes.reflect.TypeRepr): Boolean = {
    import quotes.reflect.*
    val sym = tpr.typeSymbol
    (sym.flags.is(Flags.Module) && sym.flags.is(Flags.Case)) ||
    (sym.isClassDef && sym.flags.is(Flags.Case) && caseFields(tpr).isEmpty)
  }

  private def caseFields(using Quotes)(tpr: quotes.reflect.TypeRepr): List[quotes.reflect.Symbol] = {
    import quotes.reflect.*
    val sym = tpr.typeSymbol
    if (sym.caseFields.nonEmpty) sym.caseFields
    else sym.primaryConstructor.paramSymss.flatten.filterNot(_.isTypeParam)
  }

  private def sealedChildren(using Quotes)(tpe: quotes.reflect.TypeRepr): List[quotes.reflect.TypeRepr] = {
    import quotes.reflect.*
    tpe.typeSymbol.children.flatMap { child =>
      if (child.isClassDef) Some(child.typeRef.dealias)
      else if (child.flags.is(Flags.Module)) Some(child.moduleClass.typeRef.dealias)
      else if (child.isType) Some(child.typeRef.dealias)
      else None
    }
  }

  private def moduleOrEmptyInstance[T: Type](using Quotes): Expr[T] = {
    import quotes.reflect.*
    val sym = TypeRepr.of[T].dealias.typeSymbol
    if (sym.flags.is(Flags.Module) || caseFields(TypeRepr.of[T]).isEmpty && sym.companionModule != Symbol.noSymbol) {
      if (sym.flags.is(Flags.Module)) Ref(sym.companionModule).asExprOf[T]
      else {
        val companion = Ref(sym.companionModule)
        Select.unique(companion, "apply").appliedToArgs(Nil).asExprOf[T]
      }
    } else
      Ref(sym.companionModule).asExprOf[T]
  }

  /** Resolve a field codec, deriving nested documents/enums/value classes when missing. */
  private def fieldReader[F: Type](using Quotes): Expr[BSONReader[F]] = {
    import quotes.reflect.*
    Expr
      .summon[BSONReader[F]]
      .orElse(Expr.summon[BSONDocumentReader[F]].map(e => '{ $e: BSONReader[F] }))
      .getOrElse {
        val tpr = TypeRepr.of[F].dealias
        if (EnumMacroFactory.isEnumerationValue(tpr))
          EnumMacroFactory.enumMappingOfImpl[F]
        else if (isValueClass(tpr))
          ValueClassMacroFactory.valueMappingOfImplFor[F]
        else if (
          isSupportedTrait(tpr.typeSymbol) ||
          (tpr.typeSymbol.isClassDef && tpr.typeSymbol.flags.is(Flags.Case))
        )
          '{ ${ forType[F] }: BSONReader[F] }
        else
          report.errorAndAbort(s"No BSONReader for ${tpr.show}")
      }
  }

  private def fieldWriter[F: Type](using Quotes): Expr[BSONWriter[F]] = {
    import quotes.reflect.*
    Expr
      .summon[BSONWriter[F]]
      .orElse(Expr.summon[BSONDocumentWriter[F]].map(e => '{ $e: BSONWriter[F] }))
      .getOrElse {
        val tpr = TypeRepr.of[F].dealias
        if (EnumMacroFactory.isEnumerationValue(tpr))
          EnumMacroFactory.enumMappingOfImpl[F]
        else if (isValueClass(tpr))
          ValueClassMacroFactory.valueMappingOfImplFor[F]
        else if (
          isSupportedTrait(tpr.typeSymbol) ||
          (tpr.typeSymbol.isClassDef && tpr.typeSymbol.flags.is(Flags.Case))
        )
          '{ ${ forType[F] }: BSONWriter[F] }
        else
          report.errorAndAbort(s"No BSONWriter for ${tpr.show}")
      }
  }

  private def documentReader[F: Type](using Quotes): Expr[BSONDocumentReader[F]] =
    Expr
      .summon[BSONDocumentReader[F]]
      .getOrElse('{ ${ forType[F] }: BSONDocumentReader[F] })

  private def documentWriter[F: Type](using Quotes): Expr[BSONDocumentWriter[F]] =
    Expr
      .summon[BSONDocumentWriter[F]]
      .getOrElse('{ ${ forType[F] }: BSONDocumentWriter[F] })

  private def caseObjectHandler[T: Type](using Quotes): Expr[BSONDocumentHandler[T]] = {
    val instance = moduleOrEmptyInstance[T]
    '{
      BSONDocumentHandler[T](
        _ => $instance,
        _ => BSONDocument.empty
      )
    }
  }

  private def productHandler[T: Type](using Quotes): Expr[BSONDocumentHandler[T]] = {
    import quotes.reflect.*

    val tpr    = TypeRepr.of[T].dealias
    val fields = caseFields(tpr)
    val pof    = Expr.summon[Mirror.ProductOf[T]].getOrElse {
      report.errorAndAbort(s"No Mirror.ProductOf for ${tpr.show}")
    }
    val config = macroConfig

    def readField(doc: Expr[BSONDocument], field: Symbol): Expr[Any] = {
      val fieldTpe  = tpr.memberType(field).dealias
      val rawNameEx = Expr(field.name)
      fieldTpe.asType match {
        case '[f] =>
          val reader = fieldReader[f]
          '{
            val name = $config.fieldNaming($rawNameEx)
            $doc.getAsTry[f](name)(using $reader).get
          }
      }
    }

    def writeElement(t: Expr[T], field: Symbol): Expr[BSONElement] = {
      val fieldTpe  = tpr.memberType(field).dealias
      val rawNameEx = Expr(field.name)
      fieldTpe.asType match {
        case '[f] =>
          val writer = fieldWriter[f]
          '{
            val name  = $config.fieldNaming($rawNameEx)
            val value = ${ Select(t.asTerm, field).asExprOf[f] }
            BSONElement(name, $writer.writeTry(value).get)
          }
      }
    }

    '{
      BSONDocumentHandler.from[T](
        doc =>
          scala.util.Try {
            val values: Array[Any] = Array(
              ${ Varargs(fields.map(f => readField('doc, f))) }: _*
            )
            $pof.fromProduct(Tuple.fromArray(values))
          },
        t =>
          scala.util.Try {
            BSONDocument(
              ${ Varargs(fields.map(f => writeElement('t, f))) }: _*
            )
          }
      )
    }
  }

  private def sumHandler[T: Type](using Quotes): Expr[BSONDocumentHandler[T]] = {
    import quotes.reflect.*

    val tpr      = TypeRepr.of[T].dealias
    val children = sealedChildren(tpr)
    if (children.isEmpty)
      report.errorAndAbort(s"Sealed type ${tpr.show} has no children")

    val typeName = Expr(tpr.show)
    val config   = macroConfig

    def readBranch(disc: Expr[String], doc: Expr[BSONDocument], child: TypeRepr): Expr[Option[scala.util.Try[T]]] =
      child.asType match {
        case '[c] =>
          val ct     = Expr.summon[ClassTag[c]].getOrElse {
            report.errorAndAbort(s"No ClassTag for ${child.show}")
          }
          val reader = documentReader[c]
          '{
            val expected = $config.typeNaming($ct.runtimeClass)
            if ($disc == expected) Some($reader.readTry($doc).map(_.asInstanceOf[T]))
            else None
          }
      }

    def writeBranch(value: Expr[T], child: TypeRepr): Expr[Option[scala.util.Try[BSONDocument]]] =
      child.asType match {
        case '[c] =>
          val ct     = Expr.summon[ClassTag[c]].getOrElse {
            report.errorAndAbort(s"No ClassTag for ${child.show}")
          }
          val writer = documentWriter[c]
          '{
            $value match {
              case v: c =>
                Some(
                  $writer.writeTry(v).map { childDoc =>
                    childDoc ++ BSONDocument(
                      $config.discriminator -> BSONString($config.typeNaming($ct.runtimeClass))
                    )
                  }
                )
              case _ => None
            }
          }
      }

    def readChain(disc: Expr[String], doc: Expr[BSONDocument]): Expr[scala.util.Try[T]] =
      children
        .map(c => readBranch(disc, doc, c))
        .foldRight[Expr[scala.util.Try[T]]](
          '{ Failure(new NoSuchElementException(s"Unknown discriminator '${$disc}' for ${$typeName}")) }
        ) { (attempt, rest) =>
          '{ $attempt.getOrElse($rest) }
        }

    def writeChain(value: Expr[T]): Expr[scala.util.Try[BSONDocument]] =
      children
        .map(c => writeBranch(value, c))
        .foldRight[Expr[scala.util.Try[BSONDocument]]](
          '{
            Failure(new IllegalArgumentException(s"Value ${$value} is not a known subtype of ${$typeName}"))
          }
        ) { (attempt, rest) =>
          '{ $attempt.getOrElse($rest) }
        }

    '{
      BSONDocumentHandler.from[T](
        doc =>
          $config.discriminator match {
            case discKey =>
              doc.getAsTry[String](discKey).flatMap { disc =>
                ${ readChain('disc, 'doc) }
              }
          },
        value => ${ writeChain('value) }
      )
    }
  }
}
