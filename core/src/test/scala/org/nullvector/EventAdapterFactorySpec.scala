package org.nullvector

import akka.actor.ActorSystem
import org.nullvector.domain.Money.Currency
import org.nullvector.domain._
import org.nullvector.domain.category.MainCategory.{BranchCategory2, RootCategory2, TerminalCategory2}
import org.nullvector.domain.planets.{Earth, Jupiter, ListPlanets, Mars, PlanetDistanceBetweenEarth, SolarPlanet}
import org.scalatest.Matchers._
import org.scalatest._
import reactivemongo.api.bson.MacroConfiguration.Aux
import reactivemongo.api.bson.{BSON, BSONDocument, BSONDocumentReader, BSONInteger, BSONReader, BSONString, BSONValue, BSONWriter, MacroConfiguration, MacroOptions, Macros, TypeNaming}

import scala.util.{Success, Try}

class EventAdapterFactorySpec extends FlatSpec {

  it should "create a complex mapping" in {
    val eventAdapter = EventAdapterFactory.adapt[A]("Aed")

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
    implicit val kReader: BSONDocumentReader[K] = kMapping.beforeRead({
      case BSONDocument(_) => BSONDocument("s" -> "Reader Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val eventAdapter = EventAdapterFactory.adapt[I]("Ied")
    val anInstance = I(K("k"))
    val document = eventAdapter.payloadToBson(anInstance)
    eventAdapter.bsonToPayload(document).k.s shouldBe "Reader Overrided"
  }

  it should "override Writer Mapping" in {
    val kMapping = Macros.handler[K]
    implicit val kWriter: BSONWriter[K] = kMapping.afterWrite({
      case BSONDocument(_) => BSONDocument("s" -> "Writer Overrided")
    }: PartialFunction[BSONDocument, BSONDocument])

    val justForTestTags: I => Set[String] = i => Set(s"Tag${i.k.s}")

    val eventAdapter = EventAdapterFactory.adapt[I]("Ied", justForTestTags)

    eventAdapter.tags(I(K("A"))) should contain("TagA")
    eventAdapter.tags(I(K("N"))) should contain("TagN")
    val anInstance = I(K("k"))
    eventAdapter.payloadToBson(anInstance)
      .getAsOpt[BSONDocument]("k").get
      .getAsOpt[String]("s").get shouldBe "Writer Overrided"
  }

  it should "add unsupported Mapping" in {

    implicit val writer: BSONWriter[Map[Day, String]] = (t: Map[Day, String]) => Success(BSONDocument(t.map(e => e._1.toString -> BSONString("Value_" + e._2))))
    implicit val reader: BSONReader[Map[Day, String]] = _.asTry[BSONDocument].map(_.toMap.map(e => Day(e._1) -> e._2.asOpt[String].get))

    implicit val dayMapping = new BSONReader[Day] with BSONWriter[Day] {
      override def readTry(bson: BSONValue): Try[Day] = bson.asTry[String].map(Day(_))

      override def writeTry(t: Day): Try[BSONValue] = Success(BSONString(t.toString))
    }

    val tags = Set("aTag")
    val eventAdapter = EventAdapterFactory.adapt[L]("Led", tags)
    val document = eventAdapter.payloadToBson(L(Map(Monday -> "A"), Sunday))
    val payload = eventAdapter.bsonToPayload(document)

    eventAdapter.tags(payload) should contain("aTag")
    payload.day shouldBe Sunday
    payload.m.head._2 shouldBe "Value_A"
  }

  it should "mapping sealed trit familly" in {
    val distanceFromEarthAndMars = PlanetDistanceBetweenEarth(and = Mars, kilometers = 209050000.0)

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
    val eventAdapter = EventAdapterFactory.adapt[PlanetDistanceBetweenEarth]("x")

    val document = eventAdapter.payloadToBson(distanceFromEarthAndMars)
    document.getAsOpt[BSONDocument]("and").get.getAsOpt[String]("_type").get should be("Mars")
    eventAdapter.bsonToPayload(document).and should be(Mars)
  }

  it should "mapping sealed trit familly as root event" in {
    val jupiter: SolarPlanet = Jupiter

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
    val eventAdapter = EventAdapterFactory.adapt[SolarPlanet]("x")

    val document = eventAdapter.payloadToBson(jupiter)

    document.getAsOpt[String]("_type").get should be("Jupiter")
    eventAdapter.bsonToPayload(document) should be(Jupiter)
  }

  it should "mapping sealed trit familly as root event inside class types" in {
    val listPlanets = ListPlanets(List(Jupiter, Earth, Mars))

    implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
    val eventAdapter = EventAdapterFactory.adapt[ListPlanets]("x")

    val document = eventAdapter.payloadToBson(listPlanets)
    //    println(BSONDocument.pretty(document))

    eventAdapter.bsonToPayload(document) should be(listPlanets)
  }

  it should "create EventAdapter by hand" in {
    val jupiter: SolarPlanet = Jupiter
    implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
    implicit val a: BSONDocumentMapping[SolarPlanet] = EventAdapterFactory.mappingOf[SolarPlanet]
    val eventAdapter = new EventAdapterMapping[SolarPlanet]("planet")
    val document = eventAdapter.payloadToBson(jupiter)
    document.getAsOpt[String]("_type").get should be("Jupiter")
    eventAdapter.bsonToPayload(document) should be(Jupiter)
  }

  it should "transform before read doc" in {
    implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
    implicit val mapping = EventAdapterFactory.mappingOf[SolarPlanet] { doc: BSONDocument =>
      doc.getAsOpt[String]("className") match {
        case Some(name) => doc ++ BSONDocument("_type" -> name)
        case None => doc
      }
    }
    BSON.readDocument[SolarPlanet](BSONDocument("className" -> "Mars")).get shouldBe a[Mars.type]
  }

  it should "map a case class with Enumerations" in {
    implicit val m = EventAdapterFactory.mappingOf[Product]
    val product = Product("Papitas", Money.ars(7654.345))

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
    valueMap.writeTry(new OrderId(24556)).get shouldBe BSONInteger(24556)
    valueMap.readTry(BSONInteger(24556)).get shouldBe new OrderId(24556)
  }

  it should "value class in product" in {
    //implicit val valueMap = EventAdapterFactory.valueMappingOf[OrderId]
    implicit val orderMapping = EventAdapterFactory.mappingOf[Order]

    val order = Order(OrderId(12767), Seq(Product("test", Money.ars(345))))
    val doc = BSON.writeDocument(order).get
    BSON.readDocument[Order](doc).get shouldBe order
  }

  it should "recursive mapping" in {
    import org.nullvector.domain.category.MainCategory._
    implicit val categoryMapping = EventAdapterFactory.mappingOf[Category]
    val aCategory = Category("A", List(Category("B", List(Category("C", Nil)))))
    val doc = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))

  }

  it should "recursive mapping indirect recursive Type" in {
    implicit val categoryMapping = EventAdapterFactory.mappingOf[RootCategory2]
    val aCategory = RootCategory2("Root", BranchCategory2("A", List(BranchCategory2("B", List(TerminalCategory2("C"))))))
    val doc = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))
  }

  it should "adapt recursive Type mapping" in {
    implicit val adapter = EventAdapterFactory.adapt[RootCategory2]("RecursiveTypeAdapted")
    val aCategory = RootCategory2("Root", BranchCategory2("A", List(BranchCategory2("B", List(TerminalCategory2("C"))))))
    val doc = adapter.payloadToBson(aCategory)
    println(BSONDocument.pretty(doc))
  }

  it should "mapping recursive Type mapping with before read" in {
    implicit val m = EventAdapterFactory.mappingOf[RootCategory2]((doc:BSONDocument) => doc)
    val aCategory = RootCategory2("Root", BranchCategory2("A", List(BranchCategory2("B", List(TerminalCategory2("C"))))))
    val doc = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))
  }

}


