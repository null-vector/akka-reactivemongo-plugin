package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONWriter}

import scala.reflect.macros.blackbox

private object EnumMacroFactory {

  def enumMappingOf[E](context: blackbox.Context)(implicit
      enumTypeTag: context.WeakTypeTag[E]
  ): context.Expr[BSONReader[E] with BSONWriter[E]] = {
    import context.universe._
    val aType      = enumTypeTag.tpe
    val enumType   = context.typeOf[Enumeration]
    val isEnumType = scala.util
      .Try(aType.typeSymbol.owner.asType.toType)
      .map(_ =:= enumType)
      .getOrElse(false)

    if (isEnumType)
      context.Expr[BSONReader[E] with BSONWriter[E]](apply(context)(aType))
    else
      context.abort(NoPosition, s"Type $aType not belong to $enumType")
  }

  def apply(context: blackbox.Context)(atype: context.Type): context.Tree = {
    val normalizedTypeName =
      atype.toString.split("\\.").filterNot(_ == "package")
    val enumValue          = normalizedTypeName.mkString(".")
    val enumName           = normalizedTypeName.dropRight(1).mkString(".")

    val code = context.parse(
      s"""
        import reactivemongo.api.bson._
        new BSONReader[$enumValue] with BSONWriter[$enumValue] {
          override def readTry(bson: BSONValue): scala.util.Try[$enumValue] = {
            bson.asTry[String].map(s => $enumName.withName(s))
          }
          override def writeTry(v: $enumValue): scala.util.Try[BSONValue] = {
            scala.util.Try(BSONString(v.toString))
          }
        }
      """
    )
    //println(code)
    code
  }
}
