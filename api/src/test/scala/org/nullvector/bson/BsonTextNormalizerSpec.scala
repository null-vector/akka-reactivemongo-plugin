package org.nullvector.bson

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.BSONDocument

import java.time.LocalDate

class BsonTextNormalizerSpec extends AnyFlatSpec with Matchers {

  it should """ idk """ in {

    val document = BSONDocument(
      "date" -> LocalDate.now(),
      "long" -> 1234L
    )

    println(BsonTextNormalizer(document))

  }

}
