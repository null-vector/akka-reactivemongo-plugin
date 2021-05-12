package org.nullvector

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.persistence._
import akka.persistence.journal.{EventsSeq, SingleEventSeq, Tagged, EventAdapter => AkkaEventAdapter}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import org.nullvector.util.TimeoutPromise
import reactivemongo.api.bson.BSONDocument

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {
  private var adaptersByType: immutable.HashMap[AdapterKey, Any] = immutable.HashMap()
  private var adaptersByManifest: immutable.HashMap[String, Any] = immutable.HashMap()
  private implicit val dispatcher: ExecutionContext = system.dispatchers.lookup(ReactiveMongoPlugin.pluginDispatcherName)

  private val serializerRef: ActorRef = system.systemActorOf(Props(new Actor {
    private val registryRef: ActorRef = context.actorOf(
      Props(new EventAdapterRegistry()).withDispatcher(ReactiveMongoPlugin.pluginDispatcherName), "EventAdapterRegistry")

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy()(_ => SupervisorStrategy.Restart)

    override def receive: Receive = registryRef forward _

  }), "ReactiveMongoEventSerializer")

  def serialize(persistentRepr: PersistentRepr): Future[(PersistentRepr, Set[String])] =
    persistentRepr.payload match {
      case _: BSONDocument => Future.successful(persistentRepr.withManifest(Fields.manifest_doc) -> Set.empty)
      case Tagged(realPayload, tags) =>
        val promise = TimeoutPromise[(BSONDocument, String, Set[String])](15.seconds, "Serialization has timed out.")
        serializerRef ! Serialize(realPayload, promise)
        promise.future.map(r => persistentRepr.withManifest(r._2).withPayload(r._1) -> (tags ++ r._3))

      case payload =>
        val promise = TimeoutPromise[(BSONDocument, String, Set[String])](15.seconds, "Serialization has timed out.")
        serializerRef ! Serialize(payload, promise)
        promise.future.map(r => persistentRepr.withManifest(r._2).withPayload(r._1) -> r._3)
    }

  def deserialize(manifest: String, event: BSONDocument, persistenceId: String, sequenceNumber: String): Future[Any] = {
    manifest match {
      case Fields.manifest_doc => Future.successful(event)
      case _ =>
        val promise = TimeoutPromise[Any](15.seconds, "Deserialization has timed out.")
        serializerRef ! Deserialize(manifest, event, persistenceId, sequenceNumber, promise)
        promise.future
    }
  }

  def addEventAdapter(eventAdapter: EventAdapter[_]): Unit = serializerRef ! RegisterAdapter(eventAdapter)

  def addAkkaEventAdapter(eventType: Class[_], akkaEventAdapter: AkkaEventAdapter): Unit =
    serializerRef ! RegisterAkkaAdapter(AdapterKey(eventType), akkaEventAdapter)

  def loadAkkaAdaptersFrom(path: String): Unit = {
    import scala.jdk.CollectionConverters._

    val akkaAdaptersConfig = system.settings.config.getConfig(path)
    val akkaAdapters = akkaAdaptersConfig.getConfig("event-adapters").entrySet().asScala
      .map(e => e.getKey.replace("\"", "") -> e.getValue.render().replace("\"", "")).toMap
    val eventTypesBindings = akkaAdaptersConfig.getConfig("event-adapter-bindings").entrySet().asScala
      .map(e => e.getKey.replace("\"", "") -> e.getValue.render().replace("\"", "")).toMap
    val akkaAdapterClasses = akkaAdapters.view.mapValues(value => system.dynamicAccess.getClassFor[Any](value).get.asInstanceOf[Class[AkkaEventAdapter]])

    val wrappers = eventTypesBindings
      .map((t: (String, String)) => system.dynamicAccess.getClassFor[Any](t._1).get -> akkaAdapterClasses(t._2))
      .filterNot(_._1.isAssignableFrom(classOf[BSONDocument]))
      .map((c: (Class[_], Class[AkkaEventAdapter])) => c.copy(_2 = newAkkaAdapter(c._2))).toSeq

    wrappers.foreach(e => addAkkaEventAdapter(e._1, e._2))
  }

  private def newAkkaAdapter(akkaAdapterClass: Class[AkkaEventAdapter]): AkkaEventAdapter = {
    akkaAdapterClass.getConstructors.find(_.getParameterTypes sameElements Array(classOf[ExtendedActorSystem])) match {
      case Some(constructor) => constructor.newInstance(system).asInstanceOf[AkkaEventAdapter]
      case None => akkaAdapterClass.getDeclaredConstructor().newInstance()
    }
  }


  class EventAdapterRegistry extends Actor with ActorLogging {
    var router: Router = {
      val routees = Vector.fill(Runtime.getRuntime.availableProcessors()) {
        val r = context.actorOf(Props(WorkerSerializer()))
        context.watch(r)
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    override def receive: Receive = {
      case RegisterAdapter(eventAdapter) =>
        adaptersByType = adaptersByType + (eventAdapter.eventKey -> eventAdapter)
        adaptersByManifest = adaptersByManifest + (eventAdapter.manifest -> eventAdapter)
        log.info("EventAdapter {} with manifest {} was added", eventAdapter.eventKey, eventAdapter.manifest)

      case RegisterAkkaAdapter(key, adapter) =>
        val manifest = adapter.manifest(null)
        adaptersByType = adaptersByType + (key -> adapter)
        adaptersByManifest = adaptersByManifest + (manifest -> adapter)
        log.info("Akka EventAdapter {} with manifest {} was added", key, manifest)

      case serialize: Serialize => router.route(serialize, sender())

      case deserialize: Deserialize => router.route(deserialize, sender())

      case Terminated(route) =>
        log.debug("[[Roro]] Serializer worker has been terminated")
        router = router.removeRoutee(route)
        val r = context.actorOf(Props(WorkerSerializer()))
        context.watch(r)
        router = router.addRoutee(r)
    }

    case class WorkerSerializer() extends Actor {
      override def receive: Receive = {
        case Serialize(realPayload, promise) => promise.complete(Try(
          adaptersByType.get(AdapterKey(realPayload.getClass)) match {
            case Some(adapter) => adapter match {
              case e: EventAdapter[_] => (e.toBson(realPayload), e.manifest, e.readTags(realPayload))
              case e: AkkaEventAdapter =>
                e.toJournal(realPayload) match {
                  case Tagged(payload: BSONDocument, tags) => (payload, e.manifest(realPayload), tags)
                  case payload: BSONDocument => (payload, e.manifest(realPayload), Set.empty)
                }
            }
            case None => throw new Exception(s"There is no an EventAdapter for $realPayload")
          }
        ))

        case Deserialize(manifest, document, persistenceId, sequenceNumber, promise) =>
          promise.complete(Try(
            adaptersByManifest.get(manifest) match {
              case Some(adapter) => adapter match {
                case e: EventAdapter[_] =>
                  log.debug(s"[[Roro]] Deserializing event for persistenceId:$persistenceId and sequenceNr:$sequenceNumber ")
                  e.bsonToPayload(document)
                case e: AkkaEventAdapter => e.fromJournal(document, manifest) match {
                  case SingleEventSeq(event) => event
                  case EventsSeq(event :: _) => event
                  case e => throw new Exception(e.toString)
                }
              }
              case None => throw new Exception(s"There is no an EventAdapter for $manifest")
            }
          ))
      }
    }
  }

  private case class RegisterAdapter(eventAdapter: EventAdapter[_])

  private case class RegisterAkkaAdapter(key: AdapterKey, adapter: AkkaEventAdapter)

  private case class Serialize(realPayload: Any, resultPromise: Promise[(BSONDocument, String, Set[String])])

  private case class Deserialize(manifest: String, BSONDocument: BSONDocument, persistenceId: String, sequenceNumber: String, promise: Promise[Any])

}



