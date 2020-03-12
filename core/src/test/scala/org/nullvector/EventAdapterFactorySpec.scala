package org.nullvector

import org.scalatest._
import Matchers._
import akka.actor.ActorSystem
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}

class EventAdapterFactorySpec extends FlatSpec {

  it should "create a complex mapping" in {

    val eventAdapter = EventAdatpterFactory.adapt[A]("Aed")

    val anInstance = A(B(Set(F(Some(C("Hola")))), G(List(D(23)))), C("Que"), D(34, Map("k" -> H(2.3))))
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
}

case class A(b: B, c: C, d: D)

case class B(f: Set[F], g: G)

case class C(s: String)

case class D(i: Int, m: Map[String, H] = Map.empty)

case class F(maybeC: Option[C])

case class G(ds: List[D])

case class H(d: BigDecimal)

