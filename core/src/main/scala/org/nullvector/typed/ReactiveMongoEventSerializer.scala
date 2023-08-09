package org.nullvector.typed

import akka.Done
import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.util.Timeout
import org.nullvector.*
import org.nullvector.logging.LoggerPerClassAware
import org.nullvector.typed.ReactiveMongoEventSerializer.SerializerBehavior
import reactivemongo.api.bson.BSONDocument

import scala.collection.concurrent.*
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

object ReactiveMongoEventSerializer extends ExtensionId[ReactiveMongoEventSerializer] with LoggerPerClassAware {
  override def createExtension(
      system: ActorSystem[?]
  ): ReactiveMongoEventSerializer =
    new ReactiveMongoEventSerializer(
      system.systemActorOf(
        SerializerBehavior(),
        "ReactiveMongoEventSerializer",
        DispatcherSelector.fromConfig(ReactiveMongoPlugin.pluginDispatcherName)
      )
    )(system)

  object SerializerBehavior {
    trait Command

    trait SerializationCommand extends Command

    case class AddAdapters(
        eventAdapters: Seq[EventAdapter[?]],
        replyTo: ActorRef[Done]
    ) extends Command

    case class Deserialize(
        persistentReprs: Seq[PersistentRepr],
        replyTo: ActorRef[Try[Seq[PersistentRepr]]]
    ) extends SerializationCommand

    case class DeserializePromise(
        persistentReprs: Seq[PersistentRepr],
        promise: Promise[Seq[PersistentRepr]]
    ) extends SerializationCommand

    case class Serialize(
        persistentReprs: Seq[PersistentRepr],
        replyTo: ActorRef[Try[Seq[(PersistentRepr, Set[String])]]]
    ) extends SerializationCommand

    case class SerializePromise(
        persistentReprs: Seq[PersistentRepr],
        promise: Promise[Seq[(PersistentRepr, Set[String])]]
    ) extends SerializationCommand

    def apply(): Behavior[Command] = Behaviors.setup[Command] { context =>
      val registry = new Registry()

      def serialize(persistentReprs: Seq[PersistentRepr]) = {
        persistentReprs.foldLeft[Try[Seq[(PersistentRepr, Set[String])]]](
          Success(Nil)
        )((acc, rep) =>
          acc
            .flatMap(_ => registry.adapterByPayload(rep))
            .flatMap(adapter =>
              serializeWith(adapter, rep)
                .flatMap(deRep => acc.map(_ :+ deRep))
            )
        )
      }

      def deserialize(persistentReprs: Seq[PersistentRepr]) = {
        persistentReprs.foldLeft[Try[Seq[PersistentRepr]]](Success(Nil))((acc, rep) =>
          acc
            .flatMap(_ => registry.adapterByManifest(rep))
            .flatMap(adapter =>
              deserializeWith(adapter, rep)
                .flatMap(deRep => acc.map(_ :+ deRep))
            )
        )
      }

      val workerBehavior = Behaviors
        .supervise(Behaviors.receiveMessage[SerializationCommand] {
          case Deserialize(persistentReprs, replyTo)        =>
            replyTo ! deserialize(persistentReprs)
            Behaviors.same
          case DeserializePromise(persistentReprs, promise) =>
            promise.complete(deserialize(persistentReprs))
            Behaviors.same
          case Serialize(persistentReprs, replyTo)          =>
            replyTo ! serialize(persistentReprs)
            Behaviors.same
          case SerializePromise(persistentReprs, promise)   =>
            promise.complete(serialize(persistentReprs))
            Behaviors.same
        })
        .onFailure(SupervisorStrategy.restart)

      val workerPoolBehavior = Routers
        .pool(Runtime.getRuntime.availableProcessors() * 3)(workerBehavior)
        .withRouteeProps(
          DispatcherSelector.fromConfig(
            ReactiveMongoPlugin.pluginDispatcherName
          )
        )
        .withRoundRobinRouting()

      val workerPool =
        context.spawn(workerPoolBehavior, "WorkerPool", DispatcherSelector.fromConfig(ReactiveMongoPlugin.pluginDispatcherName))

      val mainBehavior = Behaviors.receiveMessage[Command] {
        case AddAdapters(adapters, replyTo) =>
          registry.addAdapters(adapters)
          adapters.foreach(adapter =>
            logger.info(
              "EventAdapter added for type '{}' with manifest '{}'",
              adapter.eventKey.payloadType.getName,
              adapter.manifest
            )
          )
          replyTo ! Done
          Behaviors.same

        case command: SerializationCommand =>
          workerPool ! command
          Behaviors.same
      }

      Behaviors.supervise(mainBehavior).onFailure(SupervisorStrategy.restart)
    }

    def serializeWith(
        adapter: EventAdapter[_],
        persistentRepr: PersistentRepr
    ): Try[(PersistentRepr, Set[String])] =
      Try(
        persistentRepr
          .withPayload(adapter.toBson(persistentRepr.payload))
          .withManifest(adapter.manifest) -> adapter.readTags(
          persistentRepr.payload
        )
      )

    def deserializeWith(
        adapter: EventAdapter[_],
        persistentRepr: PersistentRepr
    ): Try[PersistentRepr] =
      Try(
        persistentRepr.withPayload(
          adapter.bsonToPayload(
            persistentRepr.payload.asInstanceOf[BSONDocument]
          )
        )
      )
  }

  class Registry() {
    private val adaptersByType: Map[AdapterKey, EventAdapter[_]] = TrieMap()
    private val adaptersByManifest: Map[String, EventAdapter[_]] = TrieMap()

    def addAdapters(adapters: Seq[EventAdapter[_]]) = {
      adaptersByType ++= adapters.map(adapter => adapter.eventKey -> adapter)
      adaptersByManifest ++= adapters.map(adapter => adapter.manifest -> adapter)
    }

    def adapterByManifest(
        persistentRepr: PersistentRepr
    ): Try[EventAdapter[_]] = {
      persistentRepr.manifest match {
        case Fields.manifest_doc => Success(BsonEventAdapter)
        case manifest            =>
          adaptersByManifest
            .get(manifest)
            .fold[Try[EventAdapter[_]]](failureByManifest(persistentRepr))(
              Success(_)
            )
      }
    }

    def adapterByPayload(
        persistentRepr: PersistentRepr
    ): Try[EventAdapter[_]] = {
      persistentRepr.payload match {
        case _: BSONDocument       => Success(BsonEventAdapter)
        case Tagged(payload, tags) =>
          adaptersByType
            .get(AdapterKey(payload.getClass))
            .fold[Try[EventAdapter[_]]](failureByPayload(persistentRepr))(adapter => Success(new TaggedEventAdapter(adapter, tags)))

        case payload =>
          adaptersByType
            .get(AdapterKey(payload.getClass))
            .fold[Try[EventAdapter[_]]](failureByPayload(persistentRepr))(
              Success(_)
            )
      }
    }

    private def failureByManifest(persistentRepr: PersistentRepr) = {
      Failure(
        new EventAdapterNotFound(
          s"EventAdapter for manifest '${persistentRepr.manifest}' not found."
        )
      )
    }

    private def failureByPayload(persistentRepr: PersistentRepr) = {
      Failure(
        new EventAdapterNotFound(
          s"EventAdapter for type '${persistentRepr.payload.getClass.getName}' not found."
        )
      )
    }

    class EventAdapterNotFound(textMessage: String) extends Exception(textMessage)
  }
}

class ReactiveMongoEventSerializer(
    val serializer: ActorRef[SerializerBehavior.Command]
)(implicit system: ActorSystem[?])
    extends Extension {

  private implicit val ec: ExecutionContextExecutor =
    system.dispatchers.lookup(DispatcherSelector.fromConfig(ReactiveMongoPlugin.pluginDispatcherName))

  private implicit val defaultTimeout: Timeout = Timeout(15.seconds)

  def addAdaptersAsync(adapters: Seq[EventAdapter[_]]): Future[Done] = {
    serializer
      .ask(ref => SerializerBehavior.AddAdapters(adapters, ref))
  }

  def addAdapterAsync(adapter: EventAdapter[?]): Future[Done] =
    addAdaptersAsync(Seq(adapter))

  def addAdapters(adapters: Seq[EventAdapter[?]]): Unit =
    Await.result(addAdaptersAsync(adapters), defaultTimeout.duration)

  def addAdapter(adapter: EventAdapter[?]): Unit =
    addAdapters(Seq(adapter))

  def deserialize(
      persistentReprs: Seq[PersistentRepr]
  ): Future[Seq[PersistentRepr]] = {
    val promise = Promise[Seq[PersistentRepr]]()
    serializer.tell(SerializerBehavior.DeserializePromise(persistentReprs, promise))
    promise.future
  }

  def deserialize(persistentRepr: PersistentRepr): Future[PersistentRepr] =
    deserialize(Seq(persistentRepr)).map(_.head)

  def serialize(
      persistentReprs: Seq[PersistentRepr]
  ): Future[Seq[(PersistentRepr, Set[String])]] = {
    val promise = Promise[Seq[(PersistentRepr, Set[String])]]()
    serializer.tell(SerializerBehavior.SerializePromise(persistentReprs, promise))
    promise.future
  }

  def serialize(
      persistentRepr: PersistentRepr
  ): Future[(PersistentRepr, Set[String])] =
    serialize(Seq(persistentRepr)).map(_.head)

  def deserialize(
      manifest: String,
      event: BSONDocument,
      persistenceId: String,
      sequenceNumber: Long
  ): Future[PersistentRepr] =
    deserialize(
      Seq(PersistentRepr(event, sequenceNumber, persistenceId, manifest))
    ).map(_.head)
}
