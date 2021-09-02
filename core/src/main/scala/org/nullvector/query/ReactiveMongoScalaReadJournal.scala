package org.nullvector.query

import scala.concurrent.duration.FiniteDuration

trait ReactiveMongoScalaReadJournal
    extends akka.persistence.query.scaladsl.ReadJournal
    with akka.persistence.query.scaladsl.EventsByTagQuery
    with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.CurrentEventsByTagQuery
    with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
    with akka.persistence.query.scaladsl.PersistenceIdsQuery
    with akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
    with CustomReadOps {

  val defaultRefreshInterval: FiniteDuration
}
