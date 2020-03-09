package org.nullvector.journal

import org.nullvector.Fields
import reactivemongo.api.bson._

import scala.concurrent.Future

trait ReactiveMongoHighestSequence {
  this: ReactiveMongoJournalImpl =>

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    for {
      journalSn <- journalMaxSnFrom(persistenceId, fromSequenceNr)
      snapshotSn <- snapshotMaxSnFrom(persistenceId)
    } yield List(journalSn, snapshotSn).max
  }

  private def journalMaxSnFrom(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      collection.find(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.to_sn -> BSONDocument("$gte" -> fromSequenceNr),
      ), Some(BSONDocument(Fields.to_sn -> 1)))
        .one[BSONDocument]
        .map(_.map(_.getAsOpt[Long](Fields.to_sn).get).getOrElse(0L))
    }
  }

  private def snapshotMaxSnFrom(persistenceId: String): Future[Long] = {
    rxDriver.snapshotCollection(persistenceId).flatMap { collection =>
      collection.find(BSONDocument(
        Fields.persistenceId -> persistenceId,
      ), Some(BSONDocument(Fields.sequence -> 1)))
        .one[BSONDocument]
        .map(_.map(_.getAsOpt[Long](Fields.sequence).get).getOrElse(0L))
    }
  }
}
