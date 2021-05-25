package org.nullvector
import reactivemongo.api.bson.BSONDocument

object BsonEventAdapter extends EventAdapter[BSONDocument] {
  override val manifest: String = Fields.manifest_doc

  override def payloadToBson(payload: BSONDocument): BSONDocument = payload

  override def bsonToPayload(doc: BSONDocument): BSONDocument = doc
}
