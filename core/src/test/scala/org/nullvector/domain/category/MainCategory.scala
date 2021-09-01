package org.nullvector.domain.category

object MainCategory {
  case class Category(name: String, branches: List[Category])

  sealed trait Category2 {
    val name: String
  }

  case class BranchCategory2(name: String, categories: List[Category2]) extends Category2
  case class TerminalCategory2(name: String)                            extends Category2

  case class RootCategory2(name: String, category2: Category2)

}
