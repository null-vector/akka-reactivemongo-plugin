package org.nullvector

sealed trait Tree[E] extends Iterable[E] {

  def +(tree: Tree[E]): Tree[E]

  def iterator: Iterator[E]
}


class NodeTree[E](root: E, children: List[Tree[E]]) extends Tree[E] {

  override def +(tree: Tree[E]): Tree[E] = new NodeTree[E](root, children :+ tree)

  override def iterator: Iterator[E] = (root :: children.flatMap(_.iterator)).iterator
}


object Tree {

  def apply[E](root: E, children: List[Tree[E]] = Nil): Tree[E] = new NodeTree(root, children)

  def empty[E]: Tree[E] = new Tree[E] {
    override def +(tree: Tree[E]): Tree[E] = tree

    override def iterator: Iterator[E] = Iterator.empty
  }

}