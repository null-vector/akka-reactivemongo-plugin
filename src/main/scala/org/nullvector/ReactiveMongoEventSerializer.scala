package org.nullvector

import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import reactivemongo.bson.BSONDocument

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)

}

abstract class EventAdapter[E](implicit ev: ClassTag[E]) {

  private[nullvector] def toBson(payload: Any): BSONDocument = payloadToBson(payload.asInstanceOf[E])

  val eventKey: AdapterKey[E] = AdapterKey(ev.runtimeClass.asInstanceOf[Class[E]])

  def tags(payload: Any): Set[String] = Set.empty

  val manifest: String

  def payloadToBson(payload: E): BSONDocument

  def bsonToPayload(BSONDocument: BSONDocument): E

}

case class AdapterKey[A](payloadType: Class[A]) {

  override def hashCode(): Int = payloadType.getPackage.hashCode()

  override def equals(obj: Any): Boolean = payloadType.isAssignableFrom(obj.asInstanceOf[AdapterKey[_]].payloadType)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  private val adapterRegistryRef: ActorRef = system.actorOf(Props(new EventAdapterRegistry()))

  def serialize(persistentRepr: PersistentRepr): Future[(PersistentRepr, Set[String])] =
    persistentRepr.payload match {
      case Tagged(realPayload, tags) =>
        val promise = Promise[(BSONDocument, String, Set[String])]
        adapterRegistryRef ! Serialize(realPayload, promise)
        promise.future.map(r => persistentRepr.withManifest(r._2).withPayload(r._1) -> (tags ++ r._3))

      case payload =>
        val promise = Promise[(BSONDocument, String, Set[String])]
        adapterRegistryRef ! Serialize(payload, promise)
        promise.future.map(r => persistentRepr.withManifest(r._2).withPayload(r._1) -> r._3)
    }

  def deserialize(manifest: String, event: BSONDocument): Future[Any] = {
    val promise = Promise[Any]
    adapterRegistryRef ! Deserialize(manifest, event, promise)
    promise.future

  }


  def addEventAdapter(eventAdapter: EventAdapter[_]): Unit = adapterRegistryRef ! RegisterAdapter(eventAdapter)

  class EventAdapterRegistry extends Actor {

    private val adaptersByType: mutable.HashMap[AdapterKey[_], EventAdapter[_]] = mutable.HashMap()
    private val adaptersByManifest: mutable.HashMap[String, EventAdapter[_]] = mutable.HashMap()

    override def receive: Receive = {
      case RegisterAdapter(eventAdapter) =>
        adaptersByType += eventAdapter.eventKey -> eventAdapter
        adaptersByManifest += eventAdapter.manifest -> eventAdapter

      case Serialize(realPayload, promise) =>
        adaptersByType.get(AdapterKey(realPayload.getClass)) match {
          case Some(adapter) =>
            promise.trySuccess(adapter.toBson(realPayload), adapter.manifest, adapter.tags(realPayload))
          case None => promise.tryFailure(new Exception(s"There is no an EventAdapter for $realPayload"))
        }

      case Deserialize(manifest, document, promise) =>
        adaptersByManifest.get(manifest) match {
          case Some(adapter) => promise.trySuccess(adapter.bsonToPayload(document))
          case None => promise.tryFailure(new Exception(s"There is no an EventAdapter for $manifest"))
        }
    }
  }

  case class RegisterAdapter(eventAdapter: EventAdapter[_])

  case class Serialize(realPayload: Any, resultPromise: Promise[(BSONDocument, String, Set[String])])

  case class Deserialize(manifest: String, BSONDocument: BSONDocument, promise: Promise[Any])

}

