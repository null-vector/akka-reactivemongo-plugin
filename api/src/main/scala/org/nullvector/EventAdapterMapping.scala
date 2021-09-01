package org.nullvector

import reactivemongo.api.bson.{BSON, BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.reflect.ClassTag

class EventAdapterMapping[E](val manifest: String, fTags: E => Set[String])(implicit
    mapping: BSONDocumentReader[E] with BSONDocumentWriter[E],
    ev: ClassTag[E]
) extends EventAdapter[E] {

  def this(manifest: String)(implicit
      mapping: BSONDocumentReader[E] with BSONDocumentWriter[E],
      ev: ClassTag[E]
  ) = this(manifest, _ => Set.empty)

  def this(manifest: String, tags: Set[String])(implicit
      mapping: BSONDocumentReader[E] with BSONDocumentWriter[E],
      ev: ClassTag[E]
  ) = this(manifest, _ => tags)

  override def tags(payload: E): Set[String] = fTags(payload)

  override def payloadToBson(payload: E): BSONDocument =
    BSON.writeDocument(payload).get

  override def bsonToPayload(doc: BSONDocument): E =
    BSON.readDocument[E](doc).get
}
