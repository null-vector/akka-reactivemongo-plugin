package org.nullvector

import akka.actor.ActorSystem
import org.scalatest.Matchers._
import org.scalatest._
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONString, BSONValue, Macros}

class EventAdapterFactorySpec extends FlatSpec {

  it should "create a complex mapping" in {
    val eventAdapter = EventAdatpterFactory.adapt[A]("Aed")

    val anInstance = A(
      B(Set(F(Some(C("Hola", Map("2" -> Seq(J("j"))))))),
        G(List(D(23)))),
      C("Que", Map("2" -> Seq(J("j")))),
      D(34, Map("k" -> H(2.3))),
      Seq(J("j"))
    )
    val document = eventAdapter.payloadToBson(anInstance)

    eventAdapter.manifest shouldBe "Aed"
    document.getAsOpt[BSONDocument]("d").get.getAsOpt[Int]("i").get shouldBe 34
    document
      .getAsOpt[BSONDocument]("d").get
      .getAsOpt[BSONDocument]("m").get
      .getAsOpt[BSONDocument]("k").get
      .getAsOpt[Double]("d").get shouldBe 2.3

    eventAdapter.bsonToPayload(document) shouldBe anInstance

    ReactiveMongoEventSerializer(ActorSystem()).addEventAdapter(eventAdapter)
  }

  it should "override Reader Mapping" in {
    val kMapping = Macros.handler[K]
    val kReader: BSONDocumentReader[K] = kMapping.beforeRead({
      case BSONDocument(_) => BSONDocument("s" -> "Reader Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val eventAdapter = EventAdatpterFactory.adapt[I]("Ied", kReader)
    val anInstance = I(K("k"))
    val document = eventAdapter.payloadToBson(anInstance)
    eventAdapter.bsonToPayload(document).k.s shouldBe "Reader Overrided"
  }

  it should "override Writer Mapping" in {
    val kMapping = Macros.handler[K]
    val kReader: BSONDocumentWriter[K] = kMapping.afterWrite({
      case BSONDocument(_) => BSONDocument("s" -> "Writer Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val eventAdapter = EventAdatpterFactory.adapt[I]("Ied", kReader)
    val anInstance = I(K("k"))
    eventAdapter.payloadToBson(anInstance)
      .getAsOpt[BSONDocument]("k").get
      .getAsOpt[String]("s").get shouldBe "Writer Overrided"
  }

}

case class A(b: B, c: C, d: D, js: Seq[J])

case class B(f: Set[F], g: G)

case class C(s: String, m: Map[String, Seq[J]])

case class D(i: Int, m: Map[String, H] = Map.empty)

case class F(maybeC: Option[C])

case class G(ds: List[D])

case class H(d: BigDecimal)

case class J(s: String)

case class I(k: K)

case class K(s: String)

