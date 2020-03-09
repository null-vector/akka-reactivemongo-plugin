package org.nullvector

import org.scalatest._
import Matchers._
import reactivemongo.api.bson.{BSONDocument, BSONDocumentHandler, Macros}

class EventAdapterFactorySpec extends FlatSpec {

  it should "how knows" in {

    val eventAdapter = EventAdatpterFactory.adapt[ChessboardPieceMoved]("PieceMoved")

    val document = eventAdapter.toBson(ChessboardPieceMoved("e4", Minute(1, 10)))

    eventAdapter.manifest shouldBe "PieceMoved"
    document.getAsOpt[String]("movement").get shouldBe "e4"
    document.getAsOpt[BSONDocument]("time").get.getAsOpt[Int]("minute").get shouldBe 1
  }

}

case class Minute(minute: Int, second: Int)

case class ChessboardPieceMoved(movement: String, time: Minute)
