package org.nullvector

import reactivemongo.api.bson.BSONDocument

import scala.quoted.*
import scala.reflect.ClassTag

/** Scala 3 EventAdapter macros; document codecs from [[BsonDocumentMappingDeriver]]. */
private[nullvector] object EventAdapterMacroFactory {

  def mappingOfImpl[E: Type](using Quotes): Expr[BSONDocumentMapping[E]] =
    BsonDocumentMappingDeriver.forType[E]

  def mappingOfWithBeforeReadImpl[E: Type](using
      Quotes
  )(beforeRead: Expr[BSONDocument => BSONDocument]): Expr[BSONDocumentMapping[E]] =
    '{ JoinBeforeRead[E](${ BsonDocumentMappingDeriver.forType[E] }, $beforeRead) }

  def adaptImpl[E: Type](using Quotes)(withManifest: Expr[String]): Expr[EventAdapter[E]] = {
    val classTag = summonClassTag[E]
    '{
      given ClassTag[E]             = $classTag
      given BSONDocumentMapping[E] = ${ BsonDocumentMappingDeriver.forType[E] }
      new org.nullvector.EventAdapterMapping[E]($withManifest)
    }
  }

  def adaptWithTagsImpl[E: Type](using
      Quotes
  )(withManifest: Expr[String], tags: Expr[Set[String]]): Expr[EventAdapter[E]] = {
    val classTag = summonClassTag[E]
    '{
      given ClassTag[E]             = $classTag
      given BSONDocumentMapping[E] = ${ BsonDocumentMappingDeriver.forType[E] }
      new org.nullvector.EventAdapterMapping[E]($withManifest, $tags)
    }
  }

  def adaptWithPayload2TagsImpl[E: Type](using
      Quotes
  )(withManifest: Expr[String], tags: Expr[E => Set[String]]): Expr[EventAdapter[E]] = {
    val classTag = summonClassTag[E]
    '{
      given ClassTag[E]             = $classTag
      given BSONDocumentMapping[E] = ${ BsonDocumentMappingDeriver.forType[E] }
      new org.nullvector.EventAdapterMapping[E]($withManifest, $tags)
    }
  }

  private def summonClassTag[E: Type](using Quotes): Expr[ClassTag[E]] =
    Expr.summon[ClassTag[E]].getOrElse {
      quotes.reflect.report.errorAndAbort(s"No ClassTag available for ${Type.show[E]}")
    }
}
