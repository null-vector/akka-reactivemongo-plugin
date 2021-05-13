package org.nullvector.journal

import akka.persistence.PersistentRepr
import akka.stream.{ActorAttributes, Materializer}
import org.nullvector.Fields
import org.nullvector.ReactiveMongoDriver.QueryType
import org.nullvector.ReactiveMongoPlugin.pluginDispatcherName
import org.nullvector.bson.BsonTextNormalizer
import org.nullvector.logging.LoggerPerClassAware
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait ReactiveMongoAsyncReplay extends LoggerPerClassAware {
  this: ReactiveMongoJournalImpl =>

  val parallelism = Runtime.getRuntime.availableProcessors()
  private implicit lazy val mat: Materializer = Materializer.matFromSystem(actorSystem)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                         (recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    logger.debug(s"[[Roro]] Recovering events for {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)
    rxDriver.journalCollection(persistenceId).flatMap { collection: BSONCollection =>
      val query = BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.to_sn -> BSONDocument("$gte" -> fromSequenceNr),
        Fields.from_sn -> BSONDocument("$lte" -> toSequenceNr),
      )
      val queryBuilder = collection
        .find(query)
        .hint(collection.hint(BSONDocument(
          Fields.persistenceId -> 1,
          Fields.to_sn -> 1,
          Fields.from_sn -> 1,
        )))
      rxDriver.explain(collection)(QueryType.Recovery, queryBuilder)

      queryBuilder
        .cursor[BSONDocument]()
        .collect[List](if (max >= Int.MaxValue) Int.MaxValue else max.intValue())
        .flatMap { docs =>
            val eventualsRep = docs
              .flatMap(_.getAsOpt[Seq[BSONDocument]](Fields.events).get)
              .map { doc =>
                val manifest = doc.getAsOpt[String](Fields.manifest).get
                val rawPayload = doc.getAsOpt[BSONDocument](Fields.payload).get
                val sequenceNr = doc.getAsOpt[Long](Fields.sequence).get
                serializer.deserialize(manifest, rawPayload, persistenceId, sequenceNr.toString).map(payload =>
                  PersistentRepr(
                    payload,
                    doc.getAsOpt[Long](Fields.sequence).get,
                    doc.getAsOpt[String](Fields.persistenceId).get,
                    manifest
                  )
                ) andThen {
                  case Success(_) => logger.debug(s"[[Roro]] Deserialization completed for event with persistenceId:$persistenceId and sequenceNr:$sequenceNr")
                  case Failure(_) => logger.debug(s"[[Roro]] Deserialization failed for event with persistenceId:$persistenceId and sequenceNr:$sequenceNr")
                }
              }

          Future
            .sequence(eventualsRep)
            .map(_.foreach(recoveryCallback))
        }
//      queryBuilder
//        .cursor[BSONDocument]()
//        .documentSource(if (max >= Int.MaxValue) Int.MaxValue else max.intValue())
//        .withAttributes(ActorAttributes.dispatcher(pluginDispatcherName))
//        .mapConcat(_.getAsOpt[Seq[BSONDocument]](Fields.events).get)
//        .mapAsync(1) { doc =>
//          val manifest = doc.getAsOpt[String](Fields.manifest).get
//          val rawPayload = doc.getAsOpt[BSONDocument](Fields.payload).get
//          val sequenceNr: String = doc.getAsOpt[String](Fields.from_sn).getOrElse("none")
//          serializer.deserialize(manifest, rawPayload, persistenceId, sequenceNr).map(payload =>
//            PersistentRepr(
//              payload,
//              doc.getAsOpt[Long](Fields.sequence).get,
//              doc.getAsOpt[String](Fields.persistenceId).get,
//              manifest
//            )
//          ) andThen {
//            case Success(_) => logger.debug(s"[[Roro]] Deserialization completed for event with persistenceId:$persistenceId and sequenceNr:$sequenceNr")
//            case Failure(_) => logger.debug(s"[[Roro]] Deserialization failed for event with persistenceId:$persistenceId and sequenceNr:$sequenceNr")
//          }
//        }
//        .runForeach(recoveryCallback)

    }.map { _ => }
  }


}
