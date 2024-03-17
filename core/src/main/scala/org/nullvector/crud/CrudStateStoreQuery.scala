package org.nullvector.crud

import akka.NotUsed
import akka.persistence.query.{DurableStateChange, Offset}
import akka.stream.scaladsl.Source

import scala.concurrent.duration.FiniteDuration

trait CrudStateStoreQuery[A] {
  def currentChanges(tag: Option[String], offset: Offset, consumeCursorEarly: Boolean = true): Source[DurableStateChange[A], NotUsed]
  def changes(tag: Option[String], offset: Offset, pullInterval: FiniteDuration, consumeCursorEarly: Boolean = true): Source[DurableStateChange[A], NotUsed]
}
