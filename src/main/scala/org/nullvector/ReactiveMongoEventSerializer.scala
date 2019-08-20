package org.nullvector

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.persistence._
import akka.persistence.journal.{EmptyEventSeq, EventsSeq, SingleEventSeq, Tagged, EventAdapter => AkkaEventAdapter}
import reactivemongo.bson.BSONDocument

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)

}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {

  protected implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-journal-dispatcher")

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

  def loadAkkaAdaptersFrom(path: String): Unit = {
    import scala.collection.JavaConverters._

    val akkaAdaptersConfig = system.settings.config.getConfig(path)
    val akkaAdapters = akkaAdaptersConfig.getConfig("event-adapters").entrySet().asScala
      .map(e => e.getKey.replace("\"", "") -> e.getValue.render().replace("\"", "")).toMap
    val eventTypesBindings = akkaAdaptersConfig.getConfig("event-adapter-bindings").entrySet().asScala
      .map(e => e.getKey.replace("\"", "") -> e.getValue.render().replace("\"", "")).toMap
    val akkaAdapterClasses = akkaAdapters.mapValues(value => Class.forName(value).asInstanceOf[Class[AkkaEventAdapter]])
    val wrappers = eventTypesBindings
      .map((t: (String, String)) => Class.forName(t._1) -> akkaAdapterClasses(t._2))
      .filterNot(_._1.isAssignableFrom(classOf[BSONDocument]))
      .map((c: (Class[_], Class[AkkaEventAdapter])) => new AkkaEventAdapterWrapper(c._1, c._2, system)).toSeq

    wrappers.foreach(addEventAdapter)
  }

  class EventAdapterRegistry extends Actor with ActorLogging {
    private var adaptersByType: immutable.HashMap[AdapterKey, EventAdapter[_]] = immutable.HashMap()
    private var adaptersByManifest: immutable.HashMap[String, EventAdapter[_]] = immutable.HashMap()

    override def receive: Receive = {
      case RegisterAdapter(eventAdapter) =>
        adaptersByType = adaptersByType + (eventAdapter.eventKey -> eventAdapter)
        adaptersByManifest = adaptersByManifest + (eventAdapter.manifest -> eventAdapter)
        log.info("EventAdapter {} with manifest {} was added", eventAdapter.eventKey, eventAdapter.manifest)

      case Serialize(realPayload, promise) => promise.completeWith(Future(
        adaptersByType.get(AdapterKey(realPayload.getClass)) match {
          case Some(adapter) => (adapter.toBson(realPayload), adapter.manifest, adapter.tags(realPayload))
          case None => throw new Exception(s"There is no an EventAdapter for $realPayload")
        }
      ))

      case Deserialize(manifest, document, promise) => promise.completeWith(Future(
        adaptersByManifest.get(manifest) match {
          case Some(adapter) => adapter.bsonToPayload(document)
          case None => throw new Exception(s"There is no an EventAdapter for $manifest")
        }
      ))
    }
  }

  private case class RegisterAdapter(eventAdapter: EventAdapter[_])

  private case class Serialize(realPayload: Any, resultPromise: Promise[(BSONDocument, String, Set[String])])

  private case class Deserialize(manifest: String, BSONDocument: BSONDocument, promise: Promise[Any])

}

abstract class EventAdapter[E](implicit ev: ClassTag[E]) {

  private[nullvector] def toBson(payload: Any): BSONDocument = payloadToBson(payload.asInstanceOf[E])

  val eventKey: AdapterKey = AdapterKey(ev.runtimeClass.asInstanceOf[Class[E]])

  def tags(payload: Any): Set[String] = Set.empty

  val manifest: String

  def payloadToBson(payload: E): BSONDocument

  def bsonToPayload(doc: BSONDocument): E

}

case class AdapterKey(payloadType: Class[_]) {

  override def hashCode(): Int = payloadType.getPackage.hashCode()

  override def equals(obj: Any): Boolean = payloadType.isAssignableFrom(obj.asInstanceOf[AdapterKey].payloadType)
}

class AkkaEventAdapterWrapper(eventType: Class[_], akkaAdapterClass: Class[AkkaEventAdapter], system: ExtendedActorSystem) extends EventAdapter[Any] {

  private val adapter = newAkkaAdapter

  override val eventKey: AdapterKey = AdapterKey(eventType)

  override val manifest: String = adapter.manifest(null)

  override def payloadToBson(payload: Any): BSONDocument = adapter.toJournal(payload) match {
    case Tagged(payload: BSONDocument, _) => payload
    case payload: BSONDocument => payload
    case payload => payload.asInstanceOf[BSONDocument]
  }

  override def tags(payload: Any): Set[String] = super.tags(payload)

  override def bsonToPayload(doc: BSONDocument): Any = adapter.fromJournal(doc, manifest) match {
    case SingleEventSeq(event) => event
    case EventsSeq(event :: _) => event
    case e => throw new Exception(e.toString)
  }

  def newAkkaAdapter: akka.persistence.journal.EventAdapter = {
    akkaAdapterClass.getConstructors.find(_.getParameterTypes sameElements Array(classOf[ExtendedActorSystem])) match {
      case Some(constructor) => constructor.newInstance(system).asInstanceOf[AkkaEventAdapter]
      case None => akkaAdapterClass.newInstance()
    }
  }
}