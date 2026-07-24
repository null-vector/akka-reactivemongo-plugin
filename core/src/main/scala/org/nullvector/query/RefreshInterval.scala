package org.nullvector.query

import akka.stream.Attributes

import scala.concurrent.duration.FiniteDuration

class RefreshInterval(val interval: FiniteDuration) extends Attributes.Attribute {}

object RefreshInterval {
  def apply(interval: FiniteDuration): RefreshInterval = new RefreshInterval(
    interval
  )

  // Companion conversion is picked up on Scala 3; package object keeps Scala 2 call sites working.
  implicit def refreshInterval2Attributes(refreshInterval: RefreshInterval): Attributes =
    Attributes(refreshInterval)
}
