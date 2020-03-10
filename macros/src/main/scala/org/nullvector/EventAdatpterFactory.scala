package org.nullvector

import scala.annotation.tailrec
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe

object EventAdatpterFactory {

  def adapt[E](withManifest: String): EventAdapter[E] = macro Macros.adapt[E]

  private object Macros {
    def adapt[E](context: whitebox.Context)
                (withManifest: context.Expr[String])
                (implicit typeE: context.WeakTypeTag[E]): context.Expr[EventAdapter[E]] = {

      import context.universe._

      val eventTypeName = typeE.tpe.typeSymbol.name.toTypeName
      val eventAdapterTypeName = TypeName("EventAdapter" + eventTypeName.toString)

      val handlers: Seq[context.Tree] = implicitMacroHandlersFor(context)(typeE.tpe)

      val code =
        q"""
           import reactivemongo.api.bson._
           class $eventAdapterTypeName extends org.nullvector.EventAdapter[$eventTypeName]{
              val manifest: String = $withManifest
              ..$handlers
              override def payloadToBson(payload: $eventTypeName): BSONDocument = BSON.writeDocument(payload).get
              override def bsonToPayload(doc: BSONDocument): $eventTypeName = BSON.readDocument[$eventTypeName](doc).get
           }

           new $eventAdapterTypeName
         """
      context.Expr[EventAdapter[E]](code)
    }

    private def implicitMacroHandlersFor(context: whitebox.Context)(typeE: context.universe.Type): List[context.universe.Tree] = {
      import context.universe._

      def buildExpression(valueName: context.universe.TermName, typeName: context.universe.TypeName) = {
        q""" private implicit val $valueName: BSONDocumentHandler[$typeName] = Macros.handler[$typeName] """
      }

      val namer = new Number2AphabetSeqIterator()
      extractCaseTypes(context)(typeE).toList.reverse.distinct.map { caseType =>
        val typeName = caseType.typeSymbol.name.toTypeName
        buildExpression(TermName(namer.nextName()), typeName)
      }
    }

    private val supportedClassTypes = List(
      "scala.Option",
      "scala.collection.immutable.List",
      "scala.collection.immutable.Set",
      "scala.collection.immutable.Map",
    )

    private def extractCaseTypes(context: whitebox.Context)
                                (caseType: context.universe.Type): org.nullvector.Tree[context.universe.Type] = {
      import context.universe._
      if (caseType.typeSymbol.asClass.isCaseClass) {
        Tree(caseType,
          caseType.decls.toList
            .collect { case method: MethodSymbol if method.isCaseAccessor => method }
            .collect {
              case method if method.returnType.typeSymbol.asClass.isCaseClass =>
                List(extractCaseTypes(context)(method.returnType))
              case method if supportedClassTypes.contains(method.returnType.typeSymbol.fullName) =>
                method.returnType.typeArgs.filter(_.typeSymbol.asClass.isCaseClass)
                  .map(arg => extractCaseTypes(context)(arg))

            }.flatten
        )
      } else {
        Tree.empty
      }
    }
  }

  class Number2AphabetSeqIterator(startFrom: Int = 1) {
    private val alphabet: List[Char] = 'z' :: ('a' to 'y').toList
    private val alphabetSize: Int = alphabet.size
    private val iterator: Iterator[Int] = Stream.from(startFrom).iterator

    def nextName(): String = numberToChars(iterator.next())

    private def numberToChars(aNumber: Int): String = {
      @tailrec
      def addChar(aNumber: Int, context: String): String = {
        (aNumber / alphabetSize) match {
          case 0 => alphabet(aNumber % alphabetSize) + context
          case nextNumber => addChar(nextNumber, alphabet(aNumber % alphabetSize) + context)
        }
      }

      addChar(aNumber, "")
    }

  }

}
