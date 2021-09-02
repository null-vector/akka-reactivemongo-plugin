package org.nullvector.util

import org.nullvector.util.TimeoutPromise.PromiseTimeoutException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Success

class TimeoutPromiseSpec extends AnyFlatSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits._

  it should """ dont fail """ in {
    val pumPromise = TimeoutPromise[Int](8.millis, () => "Pum")
    Thread.sleep(2)
    pumPromise.complete(Success(34))
    Await.result(pumPromise.future, 2.millis) shouldBe 34
  }

  it should """ fail """ in {
    val pumPromise = TimeoutPromise[Int](3.millis, () => "Pum")
    Thread.sleep(10)
    pumPromise.tryComplete(Success(34))
    a[PromiseTimeoutException] shouldBe thrownBy(
      Await.result(pumPromise.future, 2.millis)
    )
  }

}
