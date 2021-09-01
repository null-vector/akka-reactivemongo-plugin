package org.nullvector.journal

import akka.actor.ActorSystem
import org.nullvector.Fields
import org.nullvector.ReactiveMongoDriver.QueryType
import reactivemongo.api.bson._

import scala.concurrent.Future

trait ReactiveMongoHighestSequence {
  this: ReactiveMongoJournalImpl =>

  implicit lazy val ac: ActorSystem = this.actorSystem

  def asyncReadHighestSequenceNr(
      persistenceId: String,
      fromSequenceNr: Long
  ): Future[Long] = {
    val queries = List(
      journalMaxSnFrom(persistenceId, fromSequenceNr),
      snapshotMaxSnFrom(persistenceId)
    )
    Future.sequence(queries).map(_.max)
  }

  private def journalMaxSnFrom(
      persistenceId: String,
      fromSequenceNr: Long
  ): Future[Long] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      val queryBuilder = collection
        .find(
          BSONDocument(
            Fields.persistenceId -> persistenceId,
            Fields.to_sn         -> BSONDocument("$gte" -> fromSequenceNr)
          )
        )
        .hint(
          collection
            .hint(BSONDocument(Fields.persistenceId -> 1, Fields.to_sn -> -1))
        )
      rxDriver.explain(collection)(QueryType.HighestSeq, queryBuilder)
      queryBuilder
        .one[BSONDocument]
        .map(_.fold(0L)(_.getAsOpt[Long](Fields.to_sn).get))
    }
  }

  private def snapshotMaxSnFrom(persistenceId: String): Future[Long] = {
    rxDriver.snapshotCollection(persistenceId).flatMap { collection =>
      val queryBuilder = collection.find(
        BSONDocument(Fields.persistenceId -> persistenceId),
        Some(BSONDocument(Fields.sequence -> 1))
      )
      rxDriver.explain(collection)(QueryType.HighestSeq, queryBuilder)
      queryBuilder
        .one[BSONDocument]
        .map(_.fold(0L)(_.getAsOpt[Long](Fields.sequence).get))
    }
  }
}
