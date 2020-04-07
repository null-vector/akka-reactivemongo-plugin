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

  def adaptWithTags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[Set[String]])
                      (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(withManifest, q"override def tags(payload: Any) = $tags")
  }

  def adaptWithPayload2Tags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[Any => Set[String]])
                              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(withManifest, q"override def tags(payload: Any) = $tags(payload)")
  }

  def adapt[E](context: blackbox.Context)(withManifest: context.Expr[String])
              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    buildAdapterExpression(context)(withManifest, context.universe.EmptyTree)
  }

  private def buildAdapterExpression[E](context: blackbox.Context)
                                       (withManifest: context.Expr[String],
                                        tags: context.universe.Tree
                                       )
                                       (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {

    import context.universe._
    val eventType = eventTypeTag.tpe
    val eventAdapterTypeName = TypeName(eventType.toString + "EventAdapter")
    val handlers: Seq[context.Tree] = implicitMappingsFor(context)(eventType)
    val code =
      q"""
           import reactivemongo.api.bson._
           class $eventAdapterTypeName extends org.nullvector.EventAdapter[$eventType]{
              val manifest: String = $withManifest
              ..$handlers
              override def payloadToBson(payload: $eventType): BSONDocument = BSON.writeDocument(payload).get
              override def bsonToPayload(doc: BSONDocument): $eventType = BSON.readDocument[$eventType](doc).get
              $tags
           }
           new $eventAdapterTypeName
         """
    //println(code)
    context.Expr[EventAdapter[E]](code)
  }

  private def implicitMappingsFor(context: blackbox.Context)
                                 (eventType: context.universe.Type,
                                 ): List[context.universe.Tree] = {
    import context.universe._

    val bsonWrtterType = context.typeOf[BSONWriter[_]]
    val bsonReaderType = context.typeOf[BSONReader[_]]

    val caseClassTypes = extractCaseTypes(context)(eventType).toList.reverse.distinct

    caseClassTypes.collect {
      case caseType if context.inferImplicitValue(appliedType(bsonWrtterType, caseType)).isEmpty &&
        context.inferImplicitValue(appliedType(bsonReaderType, caseType)).isEmpty =>
        q" private implicit val ${TermName(context.freshName())}: BSONDocumentHandler[$caseType] = Macros.handler[$caseType]"
      case caseType if !context.inferImplicitValue(appliedType(bsonReaderType, caseType)).isEmpty =>
        q" private implicit val ${TermName(context.freshName())}: BSONWriter[$caseType] = Macros.handler[$caseType]"
      case caseType if !context.inferImplicitValue(appliedType(bsonWrtterType, caseType)).isEmpty =>
        q" private implicit val ${TermName(context.freshName())}: BSONReader[$caseType] = Macros.handler[$caseType]"
    }
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
