package org.nullvector

import reactivemongo.api.bson.BSONDocument

import scala.reflect.ClassTag

abstract class EventAdapter[E](implicit ev: ClassTag[E]) {

  private[nullvector] def toBson(payload: Any): BSONDocument = payloadToBson(payload.asInstanceOf[E])

  val eventKey: AdapterKey = AdapterKey(ev.runtimeClass.asInstanceOf[Class[E]])

  def tags(payload: Any): Set[String] = Set.empty

  val manifest: String

  def payloadToBson(payload: E): BSONDocument

  def bsonToPayload(doc: BSONDocument): E

}
