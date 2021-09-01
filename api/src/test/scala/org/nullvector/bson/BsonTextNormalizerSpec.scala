package org.nullvector.bson

import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.Json
import reactivemongo.api.bson.BSONDocument

import java.time.LocalDate

class BsonTextNormalizerSpec extends FlatSpec with Matchers {

  it should """ idk """ in {

    val document = BSONDocument(
      "date" -> LocalDate.now(),
      "long" -> 1234L
    )

    println(BsonTextNormalizer(document))

  }

}
