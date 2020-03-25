package org.nullvector

import akka.actor.ActorSystem
import org.scalatest.Matchers._
import org.scalatest._
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, BSONDocumentReader, BSONDocumentWriter, BSONReader, BSONString, BSONValue, BSONWriter, Macros}

import scala.util.{Success, Try}

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
    val kWriter: BSONWriter[K] = kMapping.afterWrite({
      case BSONDocument(_) => BSONDocument("s" -> "Writer Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val eventAdapter = EventAdatpterFactory.adapt[I]("Ied", kWriter)
    val anInstance = I(K("k"))
    eventAdapter.payloadToBson(anInstance)
      .getAsOpt[BSONDocument]("k").get
      .getAsOpt[String]("s").get shouldBe "Writer Overrided"
  }

  it should "add unsupported Mapping" in {

    val writer: BSONWriter[Map[Day, String]] = (t: Map[Day, String]) => Success(BSONDocument(t.map(e => e._1.toString -> BSONString(e._2))))
    val reader: BSONReader[Map[Day, String]] = _.asTry[BSONDocument].map(_.toMap.map(e => Day(e._1) -> e._2.asOpt[String].get))

    val dayMapping = new BSONReader[Day] with BSONWriter[Day] {
      override def readTry(bson: BSONValue): Try[Day] = bson.asTry[String].map(Day(_))

      override def writeTry(t: Day): Try[BSONValue] = Success(BSONString(t.toString))
    }

    val eventAdapter = EventAdatpterFactory.adapt[L]("Led", writer, reader, dayMapping)
    val document = eventAdapter.payloadToBson(L(Map(Monday -> "A"), Sunday))
    val payload = eventAdapter.bsonToPayload(document)

    payload.day shouldBe Sunday
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

case class L(m: Map[Day, String], day: Day)

sealed trait Day

object Day {
  def apply(name: String): Day = name match {
    case "Monday" => Monday
    case "Sunday" => Sunday
  }
}

case object Monday extends Day

case object Sunday extends Day
