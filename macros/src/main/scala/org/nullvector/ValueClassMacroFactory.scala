package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONWriter}

import scala.reflect.macros.blackbox

private object ValueClassMacroFactory {

  def valueMappingOf[V <: AnyVal](context: blackbox.Context)(implicit
      enumTypeTag: context.WeakTypeTag[V]
  ): context.Expr[BSONReader[V] with BSONWriter[V]] = {

    context.Expr[BSONReader[V] with BSONWriter[V]](
      apply(context)(enumTypeTag.tpe)
    )
  }

  def apply(context: blackbox.Context)(aType: context.Type): context.Tree = {
    import context.universe._
    val argMethod = aType.decls.head.asMethod

    val code = q"""
      import reactivemongo.api.bson._
      new BSONReader[$aType] with BSONWriter[$aType] {
        override def readTry(bson: BSONValue): scala.util.Try[$aType] = BSON.read[${argMethod.returnType}](bson).map(new $aType(_))
        override def writeTry(value: $aType): scala.util.Try[BSONValue] = BSON.write(value.${argMethod.name})
      }
    """
    //println(code)
    code
  }
}
