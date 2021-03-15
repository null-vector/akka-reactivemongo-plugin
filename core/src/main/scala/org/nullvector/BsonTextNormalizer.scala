package org.nullvector

import reactivemongo.api.bson.BSONDocument

object BsonTextNormalizer {

  private val valuePattern = """\s*(\w*\(('?(\w+|\d+)'?)\))\s*""".r

  def apply(doc: BSONDocument): String = {
    val bsonText = BSONDocument.pretty(doc)
    val matches = valuePattern.findAllMatchIn(bsonText)
    matches.foldLeft(bsonText)((normalizedText, aMatch) =>
      normalizedText.replace(aMatch.group(1), aMatch.group(2))
    ).replace("\"","\\\"").replace("'", "\"")
  }
}
