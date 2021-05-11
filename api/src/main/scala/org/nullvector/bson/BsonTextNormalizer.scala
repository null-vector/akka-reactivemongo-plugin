package org.nullvector.bson

import play.api.libs.json.Json
import reactivemongo.api.bson.BSONDocument

object BsonTextNormalizer {

  private val valuePattern = """\s*(\w*\(('?([^')]+)'?)\))\s*""".r

  def apply(doc: BSONDocument): String = {
    val bsonText = BSONDocument.pretty(doc)
    val matches = valuePattern.findAllMatchIn(bsonText)
    matches.foldLeft(bsonText) { (normalizedText, aMatch) =>
      normalizedText.replace(aMatch.group(1), aMatch.group(2))
    }.replaceAll("\\s*","").replace("\"","\\\"").replace("'", "\"")
  }

  def lazyBsonText(doc: BSONDocument) = new LazyJsonText(doc)

  class LazyJsonText(val doc: BSONDocument) {
    override lazy val toString = BsonTextNormalizer(doc).take(1024)
  }
}
