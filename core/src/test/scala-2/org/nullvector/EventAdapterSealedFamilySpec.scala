package org.nullvector

import org.nullvector.domain.planets._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.MacroConfiguration.Aux
import reactivemongo.api.bson._

/** Sealed case-object family tests (SolarPlanet). */
class EventAdapterSealedFamilySpec extends AnyFlatSpec with Matchers {

  it should "mapping sealed trit familly" in {
    val distanceFromEarthAndMars =
      PlanetDistanceBetweenEarth(and = Mars, kilometers = 209050000.0)

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    val eventAdapter =
      EventAdapterFactory.adapt[PlanetDistanceBetweenEarth]("x")

    val document = eventAdapter.payloadToBson(distanceFromEarthAndMars)
    document
      .getAsOpt[BSONDocument]("and")
      .get
      .getAsOpt[String]("_type")
      .get should be("Mars")
    eventAdapter.bsonToPayload(document).and should be(Mars)
  }

  it should "mapping sealed trit familly as root event" in {
    val jupiter: SolarPlanet = Jupiter

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    val eventAdapter = EventAdapterFactory.adapt[SolarPlanet]("x")

    val document = eventAdapter.payloadToBson(jupiter)

    document.getAsOpt[String]("_type").get should be("Jupiter")
    eventAdapter.bsonToPayload(document) should be(Jupiter)
  }

  it should "mapping sealed trit familly as root event inside class types" in {
    val listPlanets = ListPlanets(List(Jupiter, Earth, Mars))

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    val eventAdapter = EventAdapterFactory.adapt[ListPlanets]("x")

    val document = eventAdapter.payloadToBson(listPlanets)
    eventAdapter.bsonToPayload(document) should be(listPlanets)
  }

  it should "create EventAdapter by hand" in {
    val jupiter: SolarPlanet = Jupiter
    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    implicit val a: BSONDocumentMapping[SolarPlanet] =
      EventAdapterFactory.mappingOf[SolarPlanet]
    val eventAdapter = new EventAdapterMapping[SolarPlanet]("planet")
    val document     = eventAdapter.payloadToBson(jupiter)
    document.getAsOpt[String]("_type").get should be("Jupiter")
    eventAdapter.bsonToPayload(document) should be(Jupiter)
  }

  it should "transform before read doc" in {
    implicit val conf: Aux[MacroOptions] = MacroConfiguration(
      discriminator = "_type",
      typeNaming = TypeNaming.SimpleName
    )
    implicit val mapping: BSONDocumentMapping[SolarPlanet] =
      EventAdapterFactory.mappingOf[SolarPlanet] { (doc: BSONDocument) =>
        doc.getAsOpt[String]("className") match {
          case Some(name) => doc ++ BSONDocument("_type" -> name)
          case None       => doc
        }
      }
    BSON
      .readDocument[SolarPlanet](BSONDocument("className" -> "Mars"))
      .get shouldBe a[Mars.type]
  }
}
