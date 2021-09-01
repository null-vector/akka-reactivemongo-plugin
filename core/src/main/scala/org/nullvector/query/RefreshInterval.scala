package org.nullvector.query

import akka.stream.Attributes

import scala.concurrent.duration.FiniteDuration

class RefreshInterval(val interval: FiniteDuration) extends Attributes.Attribute {}

object RefreshInterval {
  def apply(interval: FiniteDuration): RefreshInterval = new RefreshInterval(
    interval
  )
}
