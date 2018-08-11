package org.nullvector

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {


  def serialize(manifest: String, payload: Any): Future[BSONDocument] =
    Future.successful(BSONDocument("p" -> payload.toString))


}