package org.nullvector

import reactivemongo.api.bson.{BSONReader, BSONWriter}

import scala.reflect.macros.blackbox

private object EventAdapterMacroFactory {

  def mappingOf[E](context: blackbox.Context)(implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[BSONDocumentMapping[E]] = {
    import context.universe._
    val (imports, implicits) = implicitMappingsFor(context)(eventTypeTag.tpe, noImplicitForMainType = true)
    val code =
      q"""
          import reactivemongo.api.bson._
          import org.nullvector._
          ..$imports
          ..$implicits
       """
    //println(code)
    context.Expr[BSONDocumentMapping[E]](code)
  }

  def adaptWithTags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[Set[String]])
                      (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(withManifest, q"new EventAdapterMapping($withManifest, $tags)")
  }

  def adaptWithPayload2Tags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[Any => Set[String]])
                              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(withManifest, q"new EventAdapterMapping($withManifest, $tags)")
  }

  def adapt[E](context: blackbox.Context)(withManifest: context.Expr[String])
              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(withManifest, q"new EventAdapterMapping($withManifest)")
  }

  private def buildAdapterExpression[E](context: blackbox.Context)
                                       (withManifest: context.Expr[String],
                                        createEventAdapter: context.universe.Tree
                                       )
                                       (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {

    import context.universe._
    val eventType = eventTypeTag.tpe
    val (imports, handlers) = implicitMappingsFor(context)(eventType, noImplicitForMainType = false)
    val code =
      q"""
          import reactivemongo.api.bson._
          import org.nullvector._
          ..$imports
          ..$handlers
          $createEventAdapter
      """
    //println(code)hhh
    context.Expr[EventAdapter[E]](code)
  }

  private def implicitMappingsFor(context: blackbox.Context)
                                 (eventType: context.universe.Type, noImplicitForMainType: Boolean = false
                                 ): (Set[context.Tree], List[context.Tree]) = {
    import context.universe._

    val bsonWrtterType = context.typeOf[BSONWriter[_]]
    val bsonReaderType = context.typeOf[BSONReader[_]]

    val caseClassTypes = extractCaseTypes(context)(eventType).toList.reverse.distinct

    @scala.annotation.tailrec
    def findPackage(symbol: Symbol): Option[String] = {
      symbol match {
        case NoSymbol => None
        case _ if !symbol.isPackage => findPackage(symbol.owner)
        case _ if symbol.isPackage => Some(symbol.fullName)
        case _ => None
      }
    }

    val (mappedTypes, mappingCode) = caseClassTypes.collect {
      case caseType if noImplicitForMainType & caseType =:= eventType =>
        caseType -> q"Macros.handler[$caseType]"
      case caseType if context.inferImplicitValue(appliedType(bsonWrtterType, caseType)).isEmpty && context.inferImplicitValue(appliedType(bsonReaderType, caseType)).isEmpty =>
        caseType -> q"private implicit val ${TermName(context.freshName())}: BSONDocumentHandler[${caseType}] = Macros.handler[${caseType}]"
      case caseType if !context.inferImplicitValue(appliedType(bsonReaderType, caseType)).isEmpty =>
        caseType -> q"private implicit val ${TermName(context.freshName())}: BSONWriter[${caseType}] = Macros.handler[${caseType}]"
      case caseType if !context.inferImplicitValue(appliedType(bsonWrtterType, caseType)).isEmpty =>
        caseType -> q"private implicit val ${TermName(context.freshName())}: BSONReader[${caseType}] = Macros.handler[${caseType}]"
    }.unzip
    (
      mappedTypes
        .flatMap(tpe => findPackage(tpe.typeSymbol))
        .toSet
        .map((packageName: String) => context.parse(s"import $packageName._")),
      mappingCode
    )
  }

  private def extractCaseTypes(context: blackbox.Context)
                              (caseType: context.universe.Type): org.nullvector.Tree[context.universe.Type] = {
    import context.universe._

    def isSupprtedTrait(aTypeClass: ClassSymbol) = aTypeClass.isTrait && aTypeClass.isSealed && !aTypeClass.fullName.startsWith("scala")

    def extaracCaseClassesFromTypeArgs(classType: Type): List[Type] = {
      classType.typeArgs.collect {
        case argType if argType.typeSymbol.asClass.isCaseClass => List(classType, argType)
        case t => extaracCaseClassesFromTypeArgs(t)
      }.flatten
    }

    if (caseType.typeSymbol.asClass.isCaseClass) {
      Tree(caseType,
        caseType.decls.toList
          .collect { case method: MethodSymbol if method.isCaseAccessor => method.returnType }
          .collect {
            case aType if aType.typeSymbol.asClass.isCaseClass || isSupprtedTrait(aType.typeSymbol.asClass) => List(extractCaseTypes(context)(aType))
            case aType => extaracCaseClassesFromTypeArgs(aType).map(arg => extractCaseTypes(context)(arg))
          }.flatten
      )
    }
    else if (isSupprtedTrait(caseType.typeSymbol.asClass)) {
      Tree(caseType, caseType.typeSymbol.asClass.knownDirectSubclasses.map(aType => extractCaseTypes(context)(aType.asClass.toType)).toList)
    }
    else Tree.empty
  }

}
