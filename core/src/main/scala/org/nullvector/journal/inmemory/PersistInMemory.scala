package org.nullvector.journal.inmemory

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, ExtensionId}
import org.nullvector.query.ObjectIdOffset
import reactivemongo.api.bson.BSONDocument

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Extension, ExtensionId, ActorSystem => TypedActorSystem}

object PersistInMemory extends ExtensionId[PersistInMemory] {

  sealed trait Command

  case class Persist(persistenceId: String, eventEntry: Seq[EventEntry]) extends Command

  case class Snapshot(persistenceId: String, snapshotEntry: SnapshotEntry) extends Command

  case class EventsOf(persistenceId: String, replyEvents: Either[ActorRef[Seq[EventEntry]], Promise[Seq[EventEntry]]]) extends Command

  case class SnapshotsOf(persistenceId: String, replySnapshot: Either[ActorRef[Seq[SnapshotEntry]], Promise[Seq[SnapshotEntry]]]) extends Command

  case class HighestSequenceOf(persistenceId: String, replyMaxSeq: Either[ActorRef[Long], Promise[Long]]) extends Command

  case class RemoveEventsOf(persistenceId: String, toSequenceNr: Long) extends Command

  case class EventEntry(sequence: Long, manifest: String, event: BSONDocument, tags: Set[String], offset: Option[ObjectIdOffset] = None) {
    def withOffset(): EventEntry = copy(offset = Some(ObjectIdOffset.newOffset()))
  }

  case class SnapshotEntry(sequence: Long, manifest: String, event: BSONDocument, timestamp: Long, offset: Option[ObjectIdOffset] = None) {
    def withOffset(): SnapshotEntry = copy(offset = Some(ObjectIdOffset.newOffset()))
  }

  def createExtension(system: TypedActorSystem[_]): PersistInMemory = new PersistInMemory(system)

  def get(system: TypedActorSystem[_]): PersistInMemory = apply(system)

  def behavior(): Behavior[Command] = Behaviors.setup { _ =>
    val eventsById: mutable.HashMap[String, ListBuffer[EventEntry]] = mutable.HashMap()
    val snapshotById: mutable.HashMap[String, ListBuffer[SnapshotEntry]] = mutable.HashMap()

    Behaviors.receiveMessage {
      case Persist(persistenceId, eventEntry) =>
        eventsById.get(persistenceId) match {
          case Some(events) => eventsById += (persistenceId -> (events :++ eventEntry.map(_.withOffset())))
          case None => eventsById += (persistenceId -> ListBuffer(eventEntry.map(_.withOffset()): _*))
        }
        Behaviors.same

      case Snapshot(persistenceId, snapshotEntry) =>
        snapshotById.get(persistenceId) match {
          case Some(events) => snapshotById += (persistenceId -> (events :+ snapshotEntry.withOffset()))
          case None => snapshotById += (persistenceId -> ListBuffer(snapshotEntry.withOffset()))
        }
        Behaviors.same

      case EventsOf(persistenceId, replyEvents) =>
        val events = eventsById.getOrElse(persistenceId, Nil).toSeq
        reply(replyEvents, events)
        Behaviors.same

      case SnapshotsOf(persistenceId, replySnapshots) =>
        val snapshots = snapshotById.getOrElse(persistenceId, Nil).toSeq
        reply(replySnapshots, snapshots)
        Behaviors.same

      case RemoveEventsOf(persistenceId, toSequenceNr) =>
        eventsById.get(persistenceId) match {
          case Some(events) =>
            events.dropWhileInPlace(_.sequence <= toSequenceNr)
          case None =>
        }
        Behaviors.same

      case HighestSequenceOf(persistenceId, replyMaxSeq) =>
        val maxSequence = eventsById.getOrElse(persistenceId, Nil).maxByOption(_.sequence).map(_.sequence).getOrElse(0L)
        reply(replyMaxSeq, maxSequence)
        Behaviors.same

    }
  }

  private def reply[R](refOrPromise: Either[ActorRef[R], Promise[R]], value: R) = {
    refOrPromise match {
      case Left(ref) => ref.tell(value)
      case Right(promise) => promise.success(value)
    }
  }
}

class PersistInMemory(system: TypedActorSystem[_]) extends Extension {

  import PersistInMemory._

  private val persistInMemory: ActorRef[Command] = system.systemActorOf(behavior(), "PersistInMemory")

  def addEvents(persistenceId: String, events: Seq[EventEntry]): Future[Done] = {
    persistInMemory.tell(Persist(persistenceId, events))
    Future.successful(Done)
  }

  def removeEventsTo(persistenceId: String, to: Long): Future[Done] = {
    persistInMemory.tell(RemoveEventsOf(persistenceId, to))
    Future.successful(Done)
  }

  def eventsOf(persistenceId: String): Future[Seq[EventEntry]] = {
    val promise = Promise[Seq[EventEntry]]
    persistInMemory.tell(EventsOf(persistenceId, Right(promise)))
    promise.future
  }

  def highestSequenceOf(persistenceId: String): Future[Long] = {
    val promise = Promise[Long]
    persistInMemory.tell(HighestSequenceOf(persistenceId, Right(promise)))
    promise.future
  }
}