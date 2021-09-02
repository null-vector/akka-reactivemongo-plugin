package org.nullvector

import org.nullvector.domain.Money.Currency
import org.nullvector.domain._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import reactivemongo.api.bson.{BSON, BSONDocumentHandler, BSONReader, BSONString, BSONValue, BSONWriter, Macros}

import scala.util.Try

class EnumerationMappingSpec extends AnyFlatSpec {

  it should "create a mapping for enumeration" in {

    implicit val rw =
      new BSONReader[Currency] with BSONWriter[Currency] {
        override def readTry(bson: BSONValue): Try[Currency] = {
          bson.asTry[String].map(s => org.nullvector.domain.Money.withName(s))
        }

        override def writeTry(v: Currency): Try[BSONValue] = {
          Try(BSONString(v.toString))
        }
      }

    println(BSON.write(Money.ARS))
    println(BSON.read[Money.Currency](BSONString("MXN")))

  }

}
