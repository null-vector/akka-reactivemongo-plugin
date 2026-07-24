package org.nullvector

import org.nullvector.domain.category.MainCategory._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson._

/** Recursive / sealed recursive mapping tests. */
class EventAdapterRecursiveSpec extends AnyFlatSpec with Matchers {

  it should "recursive mapping" in {
    implicit val categoryMapping: BSONDocumentMapping[Category] =
      EventAdapterFactory.mappingOf[Category]
    val aCategory = Category("A", List(Category("B", List(Category("C", Nil)))))
    val doc       = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))
  }

  it should "recursive mapping indirect recursive Type" in {
    implicit val categoryMapping: BSONDocumentMapping[RootCategory2] =
      EventAdapterFactory.mappingOf[RootCategory2]
    val aCategory = RootCategory2(
      "Root",
      BranchCategory2(
        "A",
        List(BranchCategory2("B", List(TerminalCategory2("C"))))
      )
    )
    val doc = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))
  }

  it should "adapt recursive Type mapping" in {
    implicit val adapter: EventAdapter[RootCategory2] =
      EventAdapterFactory.adapt[RootCategory2]("RecursiveTypeAdapted")
    val aCategory = RootCategory2(
      "Root",
      BranchCategory2(
        "A",
        List(BranchCategory2("B", List(TerminalCategory2("C"))))
      )
    )
    val doc = adapter.payloadToBson(aCategory)
    println(BSONDocument.pretty(doc))
  }

  it should "mapping recursive Type mapping with before read" in {
    implicit val m: BSONDocumentMapping[RootCategory2] =
      EventAdapterFactory.mappingOf[RootCategory2]((doc: BSONDocument) => doc)
    val aCategory = RootCategory2(
      "Root",
      BranchCategory2(
        "A",
        List(BranchCategory2("B", List(TerminalCategory2("C"))))
      )
    )
    val doc = BSON.writeDocument(aCategory).get
    println(BSONDocument.pretty(doc))
  }
}
