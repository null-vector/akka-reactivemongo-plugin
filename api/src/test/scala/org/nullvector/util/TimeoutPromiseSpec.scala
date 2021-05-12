package org.nullvector.util

import org.nullvector.util.TimeoutPromise.PromiseTimeoutException
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.DurationInt
import scala.util.Success

class TimeoutPromiseSpec extends FlatSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits._

  it should """ dont fail """ in {
    val pumPromise = TimeoutPromise[Int](8.millis, "Pum")
    Thread.sleep(2)
    pumPromise.completeWith(Future.successful(34))
    Await.result(pumPromise.future, 2.millis) should be
    34
  }

  it should """ fail """ in {
    val pumPromise = TimeoutPromise[Int](3.millis, "Pum")
    Thread.sleep(9)
    pumPromise.completeWith(Future.successful(34))
    a[PromiseTimeoutException] shouldBe thrownBy(
      Await.result(pumPromise.future, 2.millis)
    )
  }

}





