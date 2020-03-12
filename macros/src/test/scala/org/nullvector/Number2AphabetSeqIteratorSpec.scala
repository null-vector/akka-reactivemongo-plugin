package org.nullvector

import org.nullvector.EventAdatpterFactory.Number2AphabetSeqIterator
import org.scalatest.{FlatSpec, Matchers}

class Number2AphabetSeqIteratorSpec extends FlatSpec with Matchers {

  it should """ "a" from 1 """ in {
    val generator = new Number2AphabetSeqIterator()
    generator.nextName() shouldBe "a"
    generator.nextName() shouldBe "b"
  }

  it should """ "aa" from 27 """ in {
    val generator = new Number2AphabetSeqIterator(27)
    generator.nextName() shouldBe "aa"
    generator.nextName() shouldBe "ab"
  }

  it should """ "clf" from 2346 """ in {
    val generator = new Number2AphabetSeqIterator(2346)
    generator.nextName() shouldBe "clf"
    generator.nextName() shouldBe "clg"
  }

}


