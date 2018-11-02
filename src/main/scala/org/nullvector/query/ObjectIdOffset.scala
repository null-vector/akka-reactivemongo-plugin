package org.nullvector.query

import akka.persistence.query.Offset
import org.joda.time.DateTime
import reactivemongo.bson.BSONObjectID

object ObjectIdOffset {
  def apply(dateTime: DateTime): ObjectIdOffset = {
    val objectID = BSONObjectID.fromTime(dateTime.getMillis)
    new ObjectIdOffset(objectID)
  }
}

case class ObjectIdOffset(bsonObjectId: BSONObjectID) extends Offset with Ordered[ObjectIdOffset] {

  override val toString: String = s"id:${bsonObjectId.stringify} time:${bsonObjectId.time}"

  override def compare(that: ObjectIdOffset): Int = bsonObjectId.stringify.compare(that.bsonObjectId.stringify)
}


