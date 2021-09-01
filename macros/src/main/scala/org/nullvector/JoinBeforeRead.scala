package org.nullvector

import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler}

object JoinBeforeRead {

  def apply[T](
      handler: BSONDocumentHandler[T],
      beforeRead: BSONDocument => BSONDocument
  ): BSONDocumentHandler[T] = {
    val reader = handler.beforeRead({ case doc =>
      beforeRead(doc)
    }: PartialFunction[BSONDocument, BSONDocument])
    BSONDocumentHandler[T](
      doc => reader.readDocument(doc).get,
      t => handler.writeTry(t).get
    )
  }
}
