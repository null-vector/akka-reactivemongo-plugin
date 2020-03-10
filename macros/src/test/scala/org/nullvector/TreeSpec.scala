package org.nullvector

import org.scalatest.{FlatSpec, Matchers}

class TreeSpec extends FlatSpec with Matchers {

  it should """ has a root element """ in {
    Tree("Hola").toList shouldBe (List("Hola"))
  }

  it should """ complext tree """ in {
    val tree: Tree[String] =
      Tree("Auto", List(
        Tree("Puerta", List(Tree("Ventanilla"), Tree("Manija"))),
        Tree("Motor", List(Tree("Cilindro"), Tree("Injector"))),
        Tree("Rueda", List(Tree("Llanta"), Tree("Cubierta"))),
      ))

    tree.toList.reverse shouldBe
      List("Cubierta", "Llanta", "Rueda", "Injector", "Cilindro", "Motor", "Manija", "Ventanilla", "Puerta", "Auto")
  }

  it should """ concatenate two Tree """ in {
    (Tree("Hola", List(Tree("Que"))) + Tree("Tal")).toList shouldBe List("Hola", "Que", "Tal")
  }

  it should """ concatenate an empty tree """ in {
    (Tree("Hola", List(Tree("Que"))) + Tree.empty).toList shouldBe List("Hola", "Que")
  }

}





