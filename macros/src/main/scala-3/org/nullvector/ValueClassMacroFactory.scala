package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONValue, BSONWriter}

import scala.quoted.*

private object ValueClassMacroFactory {

  def valueMappingOfImpl[V <: AnyVal: Type](using Quotes): Expr[BSONReader[V] & BSONWriter[V]] =
    valueMappingOfImplFor[V]

  def valueMappingOfImplFor[V: Type](using Quotes): Expr[BSONReader[V] & BSONWriter[V]] = {
    import quotes.reflect.*

    val valueTpe = TypeRepr.of[V]
    val param    = valueTpe.typeSymbol.primaryConstructor.paramSymss.flatten.headOption.getOrElse {
      report.errorAndAbort(s"Type ${valueTpe.show} is not a value class with a single parameter")
    }
    val paramTpe = valueTpe.memberType(param).dealias

    paramTpe.asType match {
      case '[p] =>
        val readerExpr = Expr.summon[BSONReader[p]].getOrElse {
          report.errorAndAbort(s"No BSONReader found for ${Type.show[p]}")
        }
        val writerExpr = Expr.summon[BSONWriter[p]].getOrElse {
          report.errorAndAbort(s"No BSONWriter found for ${Type.show[p]}")
        }

        '{
          new BSONReader[V] with BSONWriter[V] {
            override def readTry(bson: BSONValue): scala.util.Try[V] =
              $readerExpr.readTry(bson).map { underlying =>
                ${
                  Apply(
                    Select(New(TypeTree.of[V]), valueTpe.typeSymbol.primaryConstructor),
                    List('{ underlying }.asTerm)
                  ).asExprOf[V]
                }
              }

            override def writeTry(value: V): scala.util.Try[BSONValue] =
              $writerExpr.writeTry(${ Select('{ value }.asTerm, param).asExprOf[p] })
          }: BSONReader[V] & BSONWriter[V]
        }
    }
  }
}
