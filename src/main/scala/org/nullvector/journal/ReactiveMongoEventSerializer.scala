package org.nullvector.journal

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import reactivemongo.bson.BSONDocument

import scala.concurrent.Future

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)

}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {

  def serialize(persistentRepr: PersistentRepr): Future[Either[PersistentRepr, (PersistentRepr, Set[String])]] =
    persistentRepr.payload match {
      case Tagged(realPayload, tags) =>
        Future.successful(Right(persistentRepr
          .withManifest("manifest")
          .withPayload(BSONDocument("p" -> realPayload.toString)) -> tags))
      case payload =>
        Future.successful(Left(persistentRepr.withManifest("manifest").withPayload(BSONDocument("p" -> payload.toString))))
    }

  def deserialize(manifest: String, event: BSONDocument): Future[Any] =
    Future.successful(event.getAs[String]("p").get)


}