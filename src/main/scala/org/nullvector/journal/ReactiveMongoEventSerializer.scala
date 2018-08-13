package org.nullvector.journal

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {


  def serialize(payload: Any): Future[(String, BSONDocument)] =
    Future.successful("manifest" ->BSONDocument("p" -> payload.toString))

  def deserialize(manifest: String, event: BSONDocument): Future[Any] =
    Future.successful(event.getAs[String]("p").get)
}