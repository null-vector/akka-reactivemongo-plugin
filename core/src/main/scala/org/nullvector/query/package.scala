package org.nullvector

import akka.stream.Attributes

package object query {
  implicit def refreshInterval2Attributes(refreshInterval: RefreshInterval): Attributes = Attributes(refreshInterval)
}
