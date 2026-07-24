package org.nullvector

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import org.nullvector.domain.Money.Currency
import org.nullvector.domain._
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson._

import scala.util.{Success, Try}

class EventAdapterFactorySpec extends AnyFlatSpec with Matchers {

  it should "create a complex mapping" in {
    val eventAdapter = EventAdapterFactory.adapt[A]("Aed")

    val anInstance = A(
      B(Set(F(Some(C("Hola", Map("2" -> Seq(J("j"))))))), G(List(D(23)))),
      C("Que", Map("2" -> Seq(J("j")))),
      D(34, Map("k" -> H(2.3))),
      Seq(J("j"))
    )
    val document   = eventAdapter.payloadToBson(anInstance)

    eventAdapter.manifest shouldBe "Aed"
    document.getAsOpt[BSONDocument]("d").get.getAsOpt[Int]("i").get shouldBe 34
    document
      .getAsOpt[BSONDocument]("d")
      .get
      .getAsOpt[BSONDocument]("m")
      .get
      .getAsOpt[BSONDocument]("k")
      .get
      .getAsOpt[Double]("d")
      .get shouldBe 2.3

    eventAdapter.bsonToPayload(document) shouldBe anInstance

    ReactiveMongoEventSerializer(ActorSystem().toTyped)
      .addAdapters(Seq(eventAdapter))
  }

  it should "override Reader Mapping" in {
    val kMapping                                = Macros.handler[K]
    implicit val kReader: BSONDocumentReader[K] = kMapping.beforeRead({ case BSONDocument(_) =>
      BSONDocument("s" -> "Reader Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val eventAdapter = EventAdapterFactory.adapt[I]("Ied")
    val anInstance   = I(K("k"))
    val document     = eventAdapter.payloadToBson(anInstance)
    eventAdapter.bsonToPayload(document).k.s shouldBe "Reader Overrided"
  }

  it should "override Writer Mapping" in {
    val kMapping                        = Macros.handler[K]
    implicit val kWriter: BSONWriter[K] = kMapping.afterWrite({ case BSONDocument(_) =>
      BSONDocument("s" -> "Writer Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val justForTestTags: I => Set[String] = i => Set(s"Tag${i.k.s}")

    val eventAdapter = EventAdapterFactory.adapt[I]("Ied", justForTestTags)

    eventAdapter.tags(I(K("A"))) should contain("TagA")
    eventAdapter.tags(I(K("N"))) should contain("TagN")
    val anInstance = I(K("k"))
    eventAdapter
      .payloadToBson(anInstance)
      .getAsOpt[BSONDocument]("k")
      .get
      .getAsOpt[String]("s")
      .get shouldBe "Writer Overrided"
  }

  it should "add unsupported Mapping" in {

    implicit val writer: BSONWriter[Map[Day, String]] = (t: Map[Day, String]) =>
      Success(
        BSONDocument(t.map(e => e._1.toString -> BSONString("Value_" + e._2)))
      )
    implicit val reader: BSONReader[Map[Day, String]] = _.asTry[BSONDocument]
      .map(_.toMap.map(e => Day(e._1) -> e._2.asOpt[String].get))

    implicit val dayMapping: BSONReader[Day] with BSONWriter[Day] =
      new BSONReader[Day] with BSONWriter[Day] {
      override def readTry(bson: BSONValue): Try[Day] =
        bson.asTry[String].map(Day(_))

      override def writeTry(t: Day): Try[BSONValue] =
        Success(BSONString(t.toString))
    }

    val tags         = Set("aTag")
    val eventAdapter = EventAdapterFactory.adapt[L]("Led", tags)
    val document     = eventAdapter.payloadToBson(L(Map(Monday -> "A"), Sunday))
    val payload      = eventAdapter.bsonToPayload(document)

    eventAdapter.tags(payload) should contain("aTag")
    payload.day shouldBe Sunday
    payload.m.head._2 shouldBe "Value_A"
  }

  it should "map a case class with Enumerations" in {
    implicit val m: BSONDocumentMapping[Product] = EventAdapterFactory.mappingOf[Product]
    val product    = Product("Papitas", Money.ars(7654.345))

    val doc = BSON.writeDocument(product).get
    println(BSONDocument.pretty(doc))
    BSON.readDocument[Product](doc).get shouldBe product
  }

  it should "direct enum mapping" in {
    val enumMapping = EventAdapterFactory.enumMappingOf[Currency]
    enumMapping.writeTry(Money.ARS).get shouldBe BSONString("ARS")
    enumMapping.readTry(BSONString("MXN")).get shouldBe Money.MXN
  }

  it should "value class mapping" in {
    val valueMap = EventAdapterFactory.valueMappingOf[OrderId]
    valueMap.writeTry(OrderId(24556)).get shouldBe BSONInteger(24556)
    valueMap.readTry(BSONInteger(24556)).get shouldBe OrderId(24556)
  }

  it should "value class in product" in {
    //implicit val valueMap = EventAdapterFactory.valueMappingOf[OrderId]
    implicit val orderMapping: BSONDocumentMapping[Order] =
      EventAdapterFactory.mappingOf[Order]

    val order = Order(OrderId(12767), Seq(Product("test", Money.ars(345))))
    val doc   = BSON.writeDocument(order).get
    BSON.readDocument[Order](doc).get shouldBe order
  }

}
