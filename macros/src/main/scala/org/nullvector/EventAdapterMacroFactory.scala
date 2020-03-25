package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONWriter}

import scala.reflect.macros.blackbox

private object EventAdapterMacroFactory {

  implicit class MapOnPair[+T1, +T2](pair: (T1, T2)) {
    def map[A1, A2](f: (T1, T2) => (A1, A2)): (A1, A2) = f(pair._1, pair._2)
  }

  private val supportedClassTypes = List(
    "scala.Option",
    "scala.collection.immutable.List",
    "scala.collection.immutable.Seq",
    "scala.collection.Seq",
    "scala.collection.immutable.Set",
    "scala.collection.immutable.Map",
  )

  def adapt[E](context: blackbox.Context)
              (withManifest: context.Expr[String], overrideMappings: context.Expr[Any]*)
              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {

    import context.universe._
    val eventType = eventTypeTag.tpe
    val eventAdapterTypeName = TypeName(eventType.toString + "EventAdapter")
    val handlers: Seq[context.Tree] = implicitMappingsFor(context)(eventType, overrideMappings)
    val code =
      q"""
           import reactivemongo.api.bson._
           class $eventAdapterTypeName extends org.nullvector.EventAdapter[$eventType]{
              val manifest: String = $withManifest
              ..$handlers
              override def payloadToBson(payload: $eventType): BSONDocument = BSON.writeDocument(payload).get
              override def bsonToPayload(doc: BSONDocument): $eventType = BSON.readDocument[$eventType](doc).get
           }
           new $eventAdapterTypeName
         """
    context.Expr[EventAdapter[E]](code)
  }

  private def implicitMappingsFor(context: blackbox.Context)
                                 (eventType: context.universe.Type,
                                  overrideMappings: Seq[context.Expr[Any]]
                                 ): List[context.universe.Tree] = {
    import context.universe._
    validateMappings(context)(overrideMappings)
    val caseClassTypes = extractCaseTypes(context)(eventType).toList.reverse.distinct
    val (overridesMap, nonOverrides) = overrideMappings
      // partitionMap is not implementend in scala 2.12
      .map(expr => expr.actualType.typeArgs.intersect(caseClassTypes) match {
        case Nil => Right(expr)
        case ::(head, _) => Left(head -> expr)
      })
      .partition(_.isLeft)
      .map((a, b) => a.map(_.left.get).groupBy(_._1) -> b.map(_.right.get)) // using left and rigth for scala 2.12 compatibility

    nonOverrides.map(expr =>
      ValDef(Modifiers(Flag.IMPLICIT | Flag.PRIVATE), TermName(context.freshName()), TypeTree(expr.actualType), expr.tree)
    ).toList :::
      caseClassTypes.flatMap { caseType =>
        overridesMap.get(caseType) match {
          case Some(overrides) => buildImplicitDeclarations(context)(TypeTree(caseType), overrides.map(_._2).toList)
          case None => buildImplicitDeclarations(context)(TypeTree(caseType), Nil)
        }
      }
  }

  private def validateMappings(context: blackbox.Context)
                              (overrides: Seq[context.Expr[Any]]): Unit = {
    import context.universe._

    overrides.foreach(expr =>
      if (!(expr.actualType <:< typeOf[BSONReader[_]] || expr.actualType <:< typeOf[BSONWriter[_]])) {
        context.abort(context.enclosingPosition,
        s""" Type ${expr.actualType} in override mapping list is no valid.
             |Must extends from ${typeOf[BSONReader[_]]} or ${typeOf[BSONWriter[_]]} or both.""".stripMargin)
      }
    )
  }

  private def buildImplicitDeclarations(context: blackbox.Context)
                                       (
                                         caseType: context.universe.TypeTree,
                                         overrides: List[context.Expr[Any]]
                                       ): List[context.universe.Tree] = {
    import context.universe._

    val valDefs = overrides.map(expr =>
      ValDef(Modifiers(Flag.IMPLICIT | Flag.PRIVATE), TermName(context.freshName()), TypeTree(expr.actualType), expr.tree))
    val implicitHandler = overrides match {
      case Nil => List(q" private implicit val ${TermName(context.freshName())}: BSONDocumentHandler[$caseType] = Macros.handler[$caseType]")
      case ::(overr, Nil) if (overr.actualType <:< typeOf[BSONReader[_]] && overr.actualType <:< typeOf[BSONWriter[_]]) => Nil
      case ::(overr, Nil) if overr.actualType <:< typeOf[BSONReader[_]] =>
        List(q" private implicit val ${TermName(context.freshName())}: BSONWriter[$caseType] = Macros.handler[$caseType]")
      case ::(overr, Nil) if overr.actualType <:< typeOf[BSONWriter[_]] =>
        List(q" private implicit val ${TermName(context.freshName())}: BSONReader[$caseType] = Macros.handler[$caseType]")
      case _ => Nil
    }
    implicitHandler ::: valDefs
  }

  private def extractCaseTypes(context: blackbox.Context)
                              (caseType: context.universe.Type): org.nullvector.Tree[context.universe.Type] = {
    import context.universe._

    def extaracCaseClassesFromSupportedTypeClasses(classType: Type): List[Type] = {
      if (supportedClassTypes.contains(classType.typeSymbol.fullName)) classType.typeArgs.collect {
        case argType if argType.typeSymbol.asClass.isCaseClass => List(classType, argType)
        case t => extaracCaseClassesFromSupportedTypeClasses(t)
      }.flatten else Nil
    }

    if (caseType.typeSymbol.asClass.isCaseClass) {
      Tree(caseType,
        caseType.decls.toList
          .collect { case method: MethodSymbol if method.isCaseAccessor => method.returnType }
          .collect {
            case aType if aType.typeSymbol.asClass.isCaseClass => List(extractCaseTypes(context)(aType))
            case aType => extaracCaseClassesFromSupportedTypeClasses(aType).map(arg => extractCaseTypes(context)(arg))
          }.flatten
      )
    }
    else Tree.empty
  }

}
