package org.nullvector

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson._

/** Scala 3 Mirror-deriver smoke tests. */
object S3MacroFixtures {
  case class Leaf(s: String)
  case class Nest(leaf: Leaf)
}

class EventAdapterFactoryMacroSpec extends AnyFlatSpec with Matchers {
  import S3MacroFixtures._

  it should "mappingOf nested case class" in {
    implicit val nest: BSONDocumentMapping[Nest] = EventAdapterFactory.mappingOf[Nest]
    val value = Nest(Leaf("x"))
    BSON.readDocument[Nest](BSON.writeDocument(value).get).get shouldBe value
  }

  it should "mappingOf with beforeRead" in {
    implicit val leaf: BSONDocumentMapping[Leaf] =
      EventAdapterFactory.mappingOf[Leaf](doc => doc ++ BSONDocument("s" -> "patched"))
    BSON.readDocument[Leaf](BSONDocument("s" -> "ignored")).get shouldBe Leaf("patched")
  }

  it should "adapt nested case class with static tags" in {
    val adapter = EventAdapterFactory.adapt[Nest]("Nest", Set("t1"))
    val value   = Nest(Leaf("x"))
    adapter.manifest shouldBe "Nest"
    adapter.tags(value) should contain("t1")
    adapter.bsonToPayload(adapter.payloadToBson(value)) shouldBe value
  }
}
