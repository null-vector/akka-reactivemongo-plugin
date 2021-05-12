package org.nullvector.snapshot

import akka.actor.ActorSystem
import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.typesafe.config.Config
import org.nullvector.ReactiveMongoDriver.QueryType
import org.nullvector.{Fields, ReactiveMongoPlugin}
import reactivemongo.api.bson._
import org.nullvector._
import org.nullvector.bson.BsonTextNormalizer
import org.nullvector.logging.LoggerPerClassAware
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ReactiveMongoSnapshotImpl(
                                 val config: Config, val actorSystem: ActorSystem
                               ) extends ReactiveMongoPlugin
  with SnapshotStoreOps with LoggerPerClassAware {

  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(actorSystem)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    logger.debug("[[Roro]] Loading Snapshot for {}", persistenceId)

    def buildQuery(collection: BSONCollection) = collection.find(
      BSONDocument(
        Fields.persistenceId -> persistenceId,
        Fields.snapshot_ts -> BSONDocument("$lte" -> criteria.maxTimestamp),
        Fields.snapshot_ts -> BSONDocument("$gte" -> criteria.minTimestamp),
        Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
        Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
      ), Option.empty[BSONDocument]
    )

    def generateSnapshotFromDocument(maybeDoc: Option[BSONDocument]) = maybeDoc match {
      case Some(document) =>
        logger.debug(s"[[Roro]] Document snapshot retrieved from mongo for persistenceId$persistenceId")

        val payloadDoc = document.getAsOpt[BSONDocument](Fields.payload) orElse
          document.getAsOpt[BSONDocument](Fields.snapshot_payload) getOrElse
          (throw new Exception(s"No payload found for snapshot with persistenceId:$persistenceId"))

        val deserializePayload = document.getAsOpt[String](Fields.manifest) match {
          case Some(manifest) =>
            val sequenceNr = document.getAsOpt[String](Fields.sequence).getOrElse("none")
            serializer.deserialize(manifest, payloadDoc, persistenceId, sequenceNr)
          case None => Future.successful(payloadDoc)
        }

        deserializePayload map { snapshot =>
          val snapshotMetadata = SnapshotMetadata(
            persistenceId,
            document.getAsOpt[Long](Fields.sequence).get,
            document.getAsOpt[Long](Fields.snapshot_ts).get,
          )
          Some(SelectedSnapshot(snapshotMetadata, snapshot))
        }
      case None =>
        logger.debug(s"[[Roro]] No Document snapshot found from mongo for persistenceId$persistenceId")
        Future.successful(None)
    }

    for {
      collection <- rxDriver.snapshotCollection(persistenceId)
      queryBuilder = buildQuery(collection)
      _ = rxDriver.explain(collection)(QueryType.LoadSnapshot, queryBuilder)
      maybeDoc <- queryBuilder.one[BSONDocument]
      maybeSnapshot <- generateSnapshotFromDocument(maybeDoc)
    } yield maybeSnapshot
  } andThen {
    case Success(Some(snapshot)) => logger.debug(s"[[Roro]] Deserialization completed for snapshot with persistenceId:$persistenceId and sequenceNr:${snapshot.metadata.sequenceNr}")
    case Success(None) => logger.debug(s"[[Roro]] Deserialization completed. No snapshot offer for persistenceId:$persistenceId")
    case Failure(_) => logger.debug(s"[[Roro]] Deserialization failed for snapshot with persistenceId:$persistenceId")
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { coll =>
      (snapshot match {
        case payload: BSONValue =>
          insertDoc(coll, metadata, payload, None)
        case _ =>
          for {
            (rep, _) <- serializer.serialize(PersistentRepr(snapshot))
            tryed <- insertDoc(coll, metadata, rep.payload.asInstanceOf[BSONDocument], Option(rep.manifest))
          } yield tryed
      }).transform(_.flatMap(identity))
    }

  }

  private def insertDoc(coll: collection.BSONCollection, metadata: SnapshotMetadata, payload: BSONValue, maybeManifest: Option[String]): Future[Try[Unit]] = {
    coll.insert(false).one(BSONDocument(
      Fields.persistenceId -> metadata.persistenceId,
      Fields.sequence -> metadata.sequenceNr,
      Fields.snapshot_ts -> metadata.timestamp,
      Fields.snapshot_payload -> payload,
      Fields.manifest -> maybeManifest,
    ))
      .map(result => if (result.writeErrors.isEmpty) Success(()) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n"))))
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    rxDriver.snapshotCollection(metadata.persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> metadata.persistenceId,
          Fields.sequence -> metadata.sequenceNr,
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)).map(result => result.errmsg match {
        case Some(error) => throw new Exception(error)
        case None => ()
      }))
    }
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    rxDriver.snapshotCollection(persistenceId).flatMap { collection =>
      val deleteBuilder = collection.delete(ordered = true)
      deleteBuilder.element(
        BSONDocument(
          Fields.persistenceId -> persistenceId,
          Fields.sequence -> BSONDocument("$gte" -> criteria.minSequenceNr),
          Fields.sequence -> BSONDocument("$lte" -> criteria.maxSequenceNr),
        ), None, None
      ).flatMap(el => deleteBuilder.many(Seq(el)).map(result => result.errmsg match {
        case Some(error) => throw new Exception(error)
        case None => ()
      }))
    }
  }

}
