package org.nullvector

import org.nullvector.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.MacroConfiguration.Aux
import reactivemongo.api.bson._

sealed trait TinyBody
case object Moon extends TinyBody
case object Star extends TinyBody

/** Scala 3 smoke for nested adapt and a tiny sealed family. */
class EventAdapterFactorySpec extends AnyFlatSpec with Matchers {

  it should "adapt nested case classes with static tags" in {
    val eventAdapter = EventAdapterFactory.adapt[I]("Ied", Set("tag-a"))
    val anInstance   = I(K("k"))
    eventAdapter.manifest shouldBe "Ied"
    eventAdapter.tags(anInstance) should contain("tag-a")
    eventAdapter.bsonToPayload(eventAdapter.payloadToBson(anInstance)) shouldBe anInstance
  }

  it should "map a small sealed case-object family as root" in {
    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    val adapter  = EventAdapterFactory.adapt[TinyBody]("body")
    val document = adapter.payloadToBson(Moon)
    document.getAsOpt[String]("_type").get shouldBe "Moon"
    adapter.bsonToPayload(document) shouldBe Moon
  }
}
