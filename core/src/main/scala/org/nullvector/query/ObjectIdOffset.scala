package org.nullvector.query

import akka.persistence.query.Offset
import org.joda.time.DateTime
import reactivemongo.api.bson.BSONObjectID

object ObjectIdOffset {
  def fromDateTime(dateTime: DateTime): ObjectIdOffset = new ObjectIdOffset(BSONObjectID.fromTime(dateTime.getMillis))
  def newOffset(): ObjectIdOffset                      = fromDateTime(DateTime.now())
}

case class ObjectIdOffset(id: BSONObjectID) extends Offset with Ordered[ObjectIdOffset] {
  override val toString: String                   = s"Offset(${id.stringify})"
  override def compare(that: ObjectIdOffset): Int = id.stringify.compare(that.id.stringify)
}
