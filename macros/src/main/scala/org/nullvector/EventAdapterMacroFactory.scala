package org.nullvector

import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, BSONReader, BSONWriter}

import scala.collection.immutable.ListSet
import scala.reflect.macros.blackbox

private object EventAdapterMacroFactory {

  def mappingOf[E](context: blackbox.Context)(implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[BSONDocumentMapping[E]] = {
    import context.universe._
    val (imports, implicits) = implicitMappingsFor(context)(eventTypeTag.tpe, noImplicitForMainType = true)
    val code =
      q"""
          import reactivemongo.api.bson._
          val f = new Function0[BSONDocumentHandler[${eventTypeTag.tpe}]] {
            ..$imports
            ..$implicits
            val h = Macros.handler[${eventTypeTag.tpe}]
            override def apply() = h
          }
          f()
       """
//    context.echo(context.enclosingPosition, s"${code.toString()}")
    context.Expr[BSONDocumentMapping[E]](code)
  }

  def mappingOfWithBeforeRead[E](context: blackbox.Context)(beforeRead: context.Expr[BSONDocument => BSONDocument])
                                (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[BSONDocumentMapping[E]] = {
    import context.universe._
    val (imports, implicits) = implicitMappingsFor(context)(eventTypeTag.tpe, noImplicitForMainType = true)
    val code =
      q"""
          import reactivemongo.api.bson._
          val f = new Function0[BSONDocumentHandler[${eventTypeTag.tpe}]] {
            ..$imports
            ..$implicits
            val handler = Macros.handler[${eventTypeTag.tpe}]
            val joined = JoinBeforeRead[${eventTypeTag.tpe}](handler, $beforeRead)
            override def apply() = joined
          }
          f()
       """
    context.Expr[BSONDocumentMapping[E]](code)
  }

  def adaptWithTags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[Set[String]])
                      (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(q"new org.nullvector.EventAdapterMapping[${eventTypeTag.tpe}]($withManifest, $tags)")
  }

  def adaptWithPayload2Tags[E](context: blackbox.Context)(withManifest: context.Expr[String], tags: context.Expr[E => Set[String]])
                              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(q"new org.nullvector.EventAdapterMapping[${eventTypeTag.tpe}]($withManifest, $tags)")
  }

  def adapt[E](context: blackbox.Context)(withManifest: context.Expr[String])
              (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {
    import context.universe._
    buildAdapterExpression(context)(q"new org.nullvector.EventAdapterMapping[${eventTypeTag.tpe}]($withManifest)")
  }

  private def buildAdapterExpression[E](context: blackbox.Context)
                                       (createEventAdapter: context.universe.Tree)
                                       (implicit eventTypeTag: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {

    import context.universe._
    val eventType = eventTypeTag.tpe
    val (imports, handlers) = implicitMappingsFor(context)(eventType, noImplicitForMainType = false)
    val code =
      q"""
        import reactivemongo.api.bson._
        val f = new Function0[org.nullvector.EventAdapter[${eventTypeTag.tpe}]] {
          ..$imports
          ..$handlers
          val adapter = $createEventAdapter
          override def apply() = adapter
        }
        f()
      """
    context.Expr[EventAdapter[E]](code)
  }

  private def implicitMappingsFor(context: blackbox.Context)
                                 (eventType: context.universe.Type, noImplicitForMainType: Boolean = false
                                 ): (Set[context.Tree], List[context.Tree]) = {
    import context.universe._

    val bsonWriterType = context.typeOf[BSONWriter[_]]
    val bsonReaderType = context.typeOf[BSONReader[_]]
    val enumType = context.typeOf[Enumeration]
    val anyValType = context.typeOf[AnyVal]
    val caseClassTypes = extractCaseTypes(context)(eventType).toList
      .foldLeft(List.empty[Type])((list, aType) => list.find(_ =:= aType).fold(aType :: list)(_ => list))

    @scala.annotation.tailrec
    def findPackage(symbol: Symbol): Option[String] = {
      symbol match {
        case NoSymbol => None
        case _ if !symbol.isPackage => findPackage(symbol.owner)
        case _ if symbol.isPackage => Some(symbol.fullName)
        case _ => None
      }
    }

    val (mappedTypes, mappingCode) = caseClassTypes.flatMap { aType =>
      val isWriterDefined = context.inferImplicitValue(appliedType(bsonWriterType, aType)).nonEmpty
      val isReaderDefined = context.inferImplicitValue(appliedType(bsonReaderType, aType)).nonEmpty
      val isEnumType = scala.util.Try(aType.typeSymbol.owner.asType.toType).map(_ =:= enumType).getOrElse(false)
      val isAnyVal = aType <:< anyValType

      (isReaderDefined, isWriterDefined) match {
        case (_, _) if noImplicitForMainType & aType =:= eventType => None

        case (false, false) =>
          Some(aType -> {
            if (isEnumType) {
              val enumMapping = EnumMacroFactory(context)(aType)
              q" implicit val ${TermName(context.freshName())}: BSONReader[${aType}] with BSONWriter[${aType}] = $enumMapping"
            } else if (isAnyVal) {
              val valueMapping = ValueClassMacroFactory(context)(aType)
              q" implicit val ${TermName(context.freshName())}: BSONReader[${aType}] with BSONWriter[${aType}] = $valueMapping"
            } else
              q" implicit val ${TermName(context.freshName())}: BSONDocumentReader[${aType}] with BSONDocumentWriter[${aType}] = Macros.handler[${aType}]"
          })

        case (true, false) =>
          Some(aType -> {
            if (isEnumType) {
              val enumMapping = EnumMacroFactory(context)(aType)
              q"private implicit val ${TermName(context.freshName())}: BSONWriter[${aType}] = $enumMapping"
            } else if (isAnyVal) {
              val valueMapping = ValueClassMacroFactory(context)(aType)
              q"private implicit val ${TermName(context.freshName())}: BSONWriter[${aType}] = $valueMapping"
            } else
              q"private implicit val ${TermName(context.freshName())}: BSONDocumentWriter[${aType}] = Macros.handler[${aType}]"
          })

        case (false, true) =>
          Some(aType -> {
            if (isEnumType) {
              val enumMapping = EnumMacroFactory(context)(aType)
              q"private implicit val ${TermName(context.freshName())}: BSONReader[${aType}] = $enumMapping"
            } else if (isAnyVal) {
              val valueMapping = ValueClassMacroFactory(context)(aType)
              q"private implicit val ${TermName(context.freshName())}: BSONReader[${aType}] = $valueMapping"
            } else
              q"private implicit val ${TermName(context.freshName())}: BSONDocumentReader[${aType}] = Macros.handler[${aType}]"
          })

        case _ => None
      }
    }
      .unzip

    mappedTypes
      .flatMap(tpe => findPackage(tpe.typeSymbol)).toSet
      .map((packageName: String) => context.parse(s"import $packageName._")) -> mappingCode
  }

  private def extractCaseTypes(context: blackbox.Context)
                              (rootType: context.universe.Type): org.nullvector.Tree[context.universe.Type] = {
    import context.universe._
    type ContextType = context.universe.Type
    val bsonWriterType = context.typeOf[BSONWriter[_]]
    val bsonReaderType = context.typeOf[BSONReader[_]]
    val enumType = context.typeOf[Enumeration]
    val anyValType = context.typeOf[AnyVal]

    def extractAll(caseType: ContextType, alreadyExtracted: List[ContextType] = Nil): org.nullvector.Tree[ContextType] = {
      def isSupportedTrait(aTypeClass: ClassSymbol) = aTypeClass.isTrait && aTypeClass.isSealed && !aTypeClass.fullName.startsWith("scala")

      def isCaseOrTrait(aType: ContextType) = aType.typeSymbol.asClass.isCaseClass || isSupportedTrait(aType.typeSymbol.asClass)

      def extractCaseClassesFromTypeArgs(classType: Type): List[Type] = {
        classType.typeArgs.collect {
          case argType if isCaseOrTrait(argType) => List(classType, argType)
          case t => extractCaseClassesFromTypeArgs(t)
        }.flatten
      }

      if (caseType.typeSymbol.asClass.isCaseClass && !alreadyExtracted.contains(caseType)) {
        Tree(caseType,
          caseType.decls.toList
            .collect { case method: MethodSymbol if method.isCaseAccessor => method.returnType }
            .collect {
              case aType if aType <:< anyValType => List(Tree(aType))
              case aType if aType.typeSymbol.owner.isType &&
                aType.typeSymbol.owner.asType.toType =:= enumType => List(Tree(aType))
              case aType if isCaseOrTrait(aType) => List(extractAll(aType, caseType :: alreadyExtracted))
              case aType => extractCaseClassesFromTypeArgs(aType)
                .map(arg => extractAll(arg, caseType :: alreadyExtracted))
            }.flatten
        )
      }
      else if (isSupportedTrait(caseType.typeSymbol.asClass)) {
        val writerNotDefined = context.inferImplicitValue(appliedType(bsonWriterType, caseType)).isEmpty
        val readerNotDefined = context.inferImplicitValue(appliedType(bsonReaderType, caseType)).isEmpty
        if ((writerNotDefined && readerNotDefined) || caseType =:= rootType)
          Tree(caseType, caseType.typeSymbol.asClass.knownDirectSubclasses.map(aType => extractAll(aType.asClass.toType, caseType :: alreadyExtracted)).toList)
        else Tree.empty
      }
      else Tree.empty
    }

    extractAll(rootType)
  }

}
