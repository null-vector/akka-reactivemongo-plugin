package org.nullvector.journal

import org.nullvector.Fields
import reactivemongo.bson.{BSONDocument, BSONString}

import scala.concurrent.Future

trait ReactiveMongoHighestSequence {
  this: ReactiveMongoJournalImpl =>

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    rxDriver.journalCollection(persistenceId).flatMap { collection =>
      import collection.BatchCommands.AggregationFramework._
      val $match = Match(BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.sequence -> BSONDocument("$gte" -> fromSequenceNr)
      ))
      val $group = Group(BSONString(s"$$${Fields.persistenceId}"))("seq" -> MaxField(Fields.sequence))

      collection.aggregatorContext[BSONDocument]($match, List($group))
        .prepared.cursor.headOption.map(_.map(_.getAs[Long]("seq").get).getOrElse(0l))
    }
}
