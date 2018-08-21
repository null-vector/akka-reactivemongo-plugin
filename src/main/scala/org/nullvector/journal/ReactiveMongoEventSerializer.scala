package org.nullvector.journal

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.journal.Tagged
import org.nullvector.journal.ReactiveMongoEventSerializer.SerializedEvent
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)

  case class SerializedEvent(manifest: String, doc: BSONDocument, tags: Set[String] = Set.empty)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {

  def serialize(payload: Any): Future[SerializedEvent] = payload match {
    case Tagged(realPayload, tags) =>
      Future.successful(SerializedEvent("manifest", BSONDocument("p" -> realPayload.toString), tags))
    case _ =>
      Future.successful(SerializedEvent("manifest", BSONDocument("p" -> payload.toString)))
  }

  def deserialize(manifest: String, event: BSONDocument): Future[Any] =
    Future.successful(event.getAs[String]("p").get)
}