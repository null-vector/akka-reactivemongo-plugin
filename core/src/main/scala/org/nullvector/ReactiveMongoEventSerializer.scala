package org.nullvector

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, OneForOneStrategy, Props, SupervisorStrategy}
import akka.persistence._
import akka.persistence.journal.{EventsSeq, SingleEventSeq, Tagged, EventAdapter => AkkaEventAdapter}
import akka.routing.BalancingPool
import cats.implicits.toTraverseOps
import org.nullvector.util.TimeoutPromise
import reactivemongo.api.bson.BSONDocument

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = ReactiveMongoEventSerializer

  override def createExtension(system: ExtendedActorSystem): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(system)
}

class ReactiveMongoEventSerializer(system: ExtendedActorSystem) extends Extension {
  private val adaptersByType: mutable.HashMap[AdapterKey, Any] = mutable.HashMap()
  private val adaptersByManifest: mutable.HashMap[String, Any] = mutable.HashMap()
  private implicit val dispatcher: ExecutionContext = system.dispatchers.lookup(ReactiveMongoPlugin.pluginDispatcherName)

  private val workerPoolRef: ActorRef = {
    val poolProps = BalancingPool(Runtime.getRuntime.availableProcessors() * 2)
      .withDispatcher(ReactiveMongoPlugin.pluginDispatcherName).props(
      restarterSupervisorProps(Props(new WorkerSerializer).withDispatcher(ReactiveMongoPlugin.pluginDispatcherName)))
    system.systemActorOf(restarterSupervisorProps(poolProps), "ReactiveMongoEventWorkerPool")
  }
  private val serializerRef: ActorRef = system.systemActorOf(
    restarterSupervisorProps(Props(new EventAdapterRegistry())), "ReactiveMongoEventRegistry")

  private val defaultSerializationTimeout: FiniteDuration = 15.seconds

  def serialize(persistentRepr: PersistentRepr): Future[(PersistentRepr, Set[String])] = {
    def promiseSerialize(tags: Set[String], payload: Any): Future[(PersistentRepr, Set[String])] = {
      val promise = TimeoutPromise[(BSONDocument, String, Set[String])](defaultSerializationTimeout,
        () => s"Serialization of event with sequence ${persistentRepr.sequenceNr} of ${persistentRepr.persistenceId} has timed out."
      )
      workerPoolRef ! Serialize(payload, promise)
      promise.future.map(r => persistentRepr.withManifest(r._2).withPayload(r._1) -> (tags ++ r._3))
    }

    persistentRepr.payload match {
      case _: BSONDocument => Future.successful(persistentRepr.withManifest(Fields.manifest_doc) -> Set.empty)
      case Tagged(realPayload, tags) => promiseSerialize(tags, realPayload)
      case payload => promiseSerialize(Set.empty, payload)
    }
  }

  def serializeAll(persistentReprs: Seq[PersistentRepr]): Future[(PersistentRepr, Set[String])] = {

    ???
  }

  def deserialize(manifest: String, event: BSONDocument, persistenceId: String, sequenceNumber: Long): Future[PersistentRepr] = {
    manifest match {
      case Fields.manifest_doc => Future.successful(PersistentRepr(event, sequenceNumber, persistenceId, manifest))
      case _ =>
        val promise = TimeoutPromise[PersistentRepr](defaultSerializationTimeout,
          () => s"Deserialization of event with sequence $sequenceNumber of ${persistenceId} has timed out.")
        workerPoolRef ! Deserialize(manifest, event, persistenceId, sequenceNumber, promise)
        promise.future
    }
  }

  def deserializeAll(from: Seq[PersistentRepr]): Future[Seq[PersistentRepr]] = {
    val promise = TimeoutPromise[Seq[PersistentRepr]](defaultSerializationTimeout,
      () => s"Deserialization of events of ${from.map(_.persistenceId).mkString(",")} has timed out.")
    workerPoolRef ! DeserializeAll(from, promise)
    promise.future
  }


  def addEventAdapter(eventAdapter: EventAdapter[_]): Unit = {
    addEventAdapters(Seq(eventAdapter))
  }

  def addEventAdapters(eventAdapter: Seq[EventAdapter[_]]): Unit = {
    val promisedDone = Promise[Unit]
    serializerRef ! RegisterAdapter(eventAdapter, promisedDone)
    Await.result(promisedDone.future, 15.seconds)
  }

  @deprecated
  def addAkkaEventAdapter(eventType: Class[_], akkaEventAdapter: AkkaEventAdapter): Unit =
    serializerRef ! RegisterAkkaAdapter(AdapterKey(eventType), akkaEventAdapter)

  @deprecated
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

  @deprecated
  private def newAkkaAdapter(akkaAdapterClass: Class[AkkaEventAdapter]): AkkaEventAdapter = {
    akkaAdapterClass.getConstructors.find(_.getParameterTypes sameElements Array(classOf[ExtendedActorSystem])) match {
      case Some(constructor) => constructor.newInstance(system).asInstanceOf[AkkaEventAdapter]
      case None => akkaAdapterClass.getDeclaredConstructor().newInstance()
    }
  }

  class EventAdapterRegistry extends Actor with ActorLogging {

    override def receive: Receive = {
      case RegisterAdapter(eventAdapter, promiseDone) =>
        promiseDone.tryComplete(Try {
          adaptersByType ++= eventAdapter.map(adapter => adapter.eventKey -> adapter)
          adaptersByManifest ++= eventAdapter.map(adapter => adapter.manifest -> adapter)
          eventAdapter.foreach(adapter =>
            log.info("EventAdapter {} with manifest {} was added", adapter.eventKey, adapter.manifest)
          )
        })

      case RegisterAkkaAdapter(key, adapter) =>
        val manifest = adapter.manifest(null)
        log.warning("Registration of Akka EventAdapter is deprecated")
    }

  }

  class WorkerSerializer extends Actor with ActorLogging {
    override def receive: Receive = {
      case Serialize(realPayload, promise) => promise.tryComplete(Try {

        adaptersByType.get(AdapterKey(realPayload.getClass)) match {
          case Some(adapter) =>
            log.debug("Serializing event with {}", adapter)
            adapter match {
              case e: EventAdapter[_] => (e.toBson(realPayload), e.manifest, e.readTags(realPayload))
              case e: AkkaEventAdapter =>
                e.toJournal(realPayload) match {
                  case Tagged(payload: BSONDocument, tags) => (payload, e.manifest(realPayload), tags)
                  case payload: BSONDocument => (payload, e.manifest(realPayload), Set.empty)
                }
            }
          case None => throw new Exception(s"There is no an EventAdapter for $realPayload")
        }
      })

      case Deserialize(manifest, document, persistenceId, sequenceNumber, promise) =>
        promise.tryComplete(deserialize(PersistentRepr(document, sequenceNumber, persistenceId, manifest)))

      case DeserializeAll(from, promise) =>
        promise.tryComplete(from.map(deserialize).sequence)

    }

    private def serialize(persistentRepr: PersistentRepr): Try[PersistentRepr] = {
      persistentRepr.payload
      ???
    }

    private def deserialize(persistentRepr: PersistentRepr): Try[PersistentRepr] = {
      if (persistentRepr.manifest == Fields.manifest_doc)
        Success(persistentRepr)
      else
        adaptersByManifest.get(persistentRepr.manifest) match {
          case Some(adapter) =>
            log.debug("Deserializing event with {}", adapter)
            adapter match {
              case eventAdapter: EventAdapter[_] =>
                Success(persistentRepr.withPayload(eventAdapter.bsonToPayload(persistentRepr.payload.asInstanceOf[BSONDocument])))
              case akkaEventAdapter: AkkaEventAdapter =>
                akkaEventAdapter.fromJournal(persistentRepr.payload, persistentRepr.manifest) match {
                  case SingleEventSeq(event) => Success(persistentRepr.withPayload(event))
                  case EventsSeq(event :: _) => Success(persistentRepr.withPayload(event))
                  case other => Failure(new Exception(other.toString))
                }
            }
          case None => Failure(new Exception(s"There is no an EventAdapter for $manifest"))
        }
    }
  }

  private def restarterSupervisorProps(props: Props) = {
    Props(new Actor {
      private val supervisedRef: ActorRef = context.actorOf(props.withDispatcher(ReactiveMongoPlugin.pluginDispatcherName), "Supervised")

      override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy()(_ => SupervisorStrategy.Restart)

      override def receive: Receive = supervisedRef forward _
    })
  }

  private case class RegisterAdapter(eventAdapter: Seq[EventAdapter[_]], promise: Promise[Unit])

  private case class RegisterAkkaAdapter(key: AdapterKey, adapter: AkkaEventAdapter)

  private case class Serialize(realPayload: Any, resultPromise: Promise[(BSONDocument, String, Set[String])])

  private case class SerializeAll(payloads: Seq[Any], resultPromise: Promise[Seq[(BSONDocument, String, Set[String])]])

  private case class Deserialize(manifest: String, BSONDocument: BSONDocument,
                                 persistenceId: String, sequenceNumber: Long, promise: Promise[PersistentRepr])

  private case class DeserializeAll(from: Seq[PersistentRepr], promise: Promise[Seq[PersistentRepr]])

}



