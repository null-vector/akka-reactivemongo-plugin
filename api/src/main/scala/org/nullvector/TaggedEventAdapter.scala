package org.nullvector

import akka.persistence.journal.Tagged
import reactivemongo.api.bson.BSONDocument

import scala.reflect.ClassTag

class TaggedEventAdapter[E](adapter: EventAdapter[E], tags: Set[String])
                           (implicit ev: ClassTag[E]) extends EventAdapter[E] {

  override val manifest: String = adapter.manifest

  override def tags(payload: E): Set[String] = tags

  override def payloadToBson(payload: E): BSONDocument = payload match {
    case Tagged(realPayload, _) => adapter.toBson(realPayload)
    case _ => adapter.payloadToBson(payload)
  }

  override def bsonToPayload(doc: BSONDocument): E = adapter.bsonToPayload(doc)

}

