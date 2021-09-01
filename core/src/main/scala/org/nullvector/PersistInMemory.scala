package org.nullvector

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, Extension, ExtensionId, ActorSystem => TypedActorSystem}
import akka.persistence.query.Offset
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import org.nullvector.query.ObjectIdOffset
import reactivemongo.api.bson.BSONDocument

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

object PersistInMemory extends ExtensionId[PersistInMemory] {

  sealed trait Command

  case class Persist(
      persistenceId: String,
      eventEntries: Seq[EventEntry],
      replyDone: Either[ActorRef[Done], Promise[Done]]
  ) extends Command

  case class Snapshot(
      persistenceId: String,
      snapshotEntry: SnapshotEntry,
      replyDone: Either[ActorRef[Done], Promise[Done]]
  ) extends Command

  case class EventsOf(
      persistenceId: String,
      replyEvents: Either[ActorRef[Seq[EventWithOffset]], Promise[
        Seq[EventWithOffset]
      ]]
  ) extends Command

  case class AllEvents(
      from: Offset,
      replyEvents: Either[ActorRef[Source[EventWithOffset, NotUsed]], Promise[
        Source[EventWithOffset, NotUsed]
      ]]
  ) extends Command

  case class AllPersistenceIds(
      replyEvents: Either[ActorRef[Source[String, NotUsed]], Promise[
        Source[String, NotUsed]
      ]]
  ) extends Command

  case class SnapshotsOf(
      persistenceId: String,
      replySnapshot: Either[ActorRef[Seq[SnapshotEntry]], Promise[
        Seq[SnapshotEntry]
      ]]
  ) extends Command

  case class HighestSequenceOf(
      persistenceId: String,
      replyMaxSeq: Either[ActorRef[Long], Promise[Long]]
  ) extends Command

  case class RemoveEventsOf(
      persistenceId: String,
      toSequenceNr: Long,
      replyDone: Either[ActorRef[Done], Promise[Done]]
  ) extends Command

  case class RemoveSnapshotsOf(
      persistenceId: String,
      sequences: SequenceRange,
      replyDone: Either[ActorRef[Done], Promise[Done]]
  ) extends Command

  case class InvalidateAll(replyDone: Either[ActorRef[Done], Promise[Done]]) extends Command

  case class EventEntry(
      persistenceId: String,
      sequence: Long,
      manifest: String,
      event: BSONDocument,
      tags: Set[String]
  )

  case class EventWithOffset(eventEntry: EventEntry, offset: ObjectIdOffset) extends Ordered[EventWithOffset] {
    override def compare(that: EventWithOffset): Int =
      offset.compare(that.offset)
  }

  case class SnapshotEntry(
      sequence: Long,
      manifest: String,
      event: BSONDocument,
      timestamp: Long
  )

  class SequenceRange(min: Long, max: Long) {
    def this(single: Long) = this(single, single)

    def contains(value: Long) = min <= value && max >= value
  }

  def createExtension(system: TypedActorSystem[_]): PersistInMemory =
    new PersistInMemory(system)

  def get(system: TypedActorSystem[_]): PersistInMemory = apply(system)

  def behavior(): Behavior[Command] = Behaviors.setup { _ =>
    val eventsById: mutable.HashMap[String, ListBuffer[EventWithOffset]] =
      mutable.HashMap()
    val snapshotById: mutable.HashMap[String, ListBuffer[SnapshotEntry]] =
      mutable.HashMap()

    Behaviors.receiveMessage {
      case Persist(persistenceId, eventEntries, replyDone) =>
        val eventsWithOffset = eventEntries.map(EventWithOffset(_, newOffset()))
        eventsById.get(persistenceId) match {
          case Some(events) =>
            events.appendAll(eventsWithOffset)
          case None         =>
            eventsById += (persistenceId -> ListBuffer(eventsWithOffset: _*))
        }
        reply(replyDone, Done)
        Behaviors.same

      case Snapshot(persistenceId, snapshotEntry, replyDone) =>
        snapshotById.get(persistenceId) match {
          case Some(events) => events append snapshotEntry
          case None         =>
            snapshotById += (persistenceId -> ListBuffer(snapshotEntry))
        }
        reply(replyDone, Done)
        Behaviors.same

      case EventsOf(persistenceId, replyEvents) =>
        val events = eventsById.getOrElse(persistenceId, Nil).toSeq
        reply(replyEvents, events)
        Behaviors.same

      case SnapshotsOf(persistenceId, replySnapshots) =>
        val snapshots = snapshotById.getOrElse(persistenceId, Nil).toSeq
        reply(replySnapshots, snapshots)
        Behaviors.same

      case RemoveEventsOf(persistenceId, toSequenceNr, replyDone) =>
        eventsById.get(persistenceId) match {
          case Some(events) =>
            events.dropWhileInPlace(_.eventEntry.sequence <= toSequenceNr)
          case None         =>
        }
        reply(replyDone, Done)
        Behaviors.same

      case HighestSequenceOf(persistenceId, replyMaxSeq) =>
        val maxEventSeq    = eventsById
          .getOrElse(persistenceId, Nil)
          .lastOption
          .fold(0L)(_.eventEntry.sequence)
        val maxSnapshotSeq = snapshotById
          .getOrElse(persistenceId, Nil)
          .lastOption
          .fold(0L)(_.sequence)
        reply(replyMaxSeq, List(maxEventSeq, maxSnapshotSeq).max)
        Behaviors.same

      case RemoveSnapshotsOf(persistenceId, sequences, replyDone) =>
        snapshotById.get(persistenceId) match {
          case Some(snapshots) =>
            snapshots.filterInPlace(entry => !sequences.contains(entry.sequence))
          case None            =>
        }
        reply(replyDone, Done)
        Behaviors.same

      case AllEvents(from, replyEvents) =>
        val allEvents = eventsById.view.values.flatten
        val filtered  = from match {
          case offset: ObjectIdOffset => allEvents.filter(_.offset > offset)
          case _                      => allEvents
        }
        val source    = Source(filtered.toList.sorted)
        reply(replyEvents, source)
        Behaviors.same

      case AllPersistenceIds(replyEvents) =>
        val source = Source.fromIterator(() => eventsById.keysIterator)
        reply(replyEvents, source)
        Behaviors.same

      case InvalidateAll(replyDone) =>
        eventsById.clear()
        snapshotById.clear()
        reply(replyDone, Done)
        Behaviors.same
    }
  }

  private def newOffset(): ObjectIdOffset = ObjectIdOffset.newOffset()

  private def reply[R](
      refOrPromise: Either[ActorRef[R], Promise[R]],
      value: R
  ) = {
    refOrPromise match {
      case Left(ref)      => ref.tell(value)
      case Right(promise) => promise.success(value)
    }
  }
}

class PersistInMemory(system: TypedActorSystem[_]) extends Extension {

  import PersistInMemory._

  private val persistInMemory: ActorRef[Command] = system.systemActorOf(
    behavior(),
    "PersistInMemory",
    DispatcherSelector.fromConfig(
      "akka-persistence-reactivemongo.persist-in-memory-dispatcher"
    )
  )

  def addEvents(
      persistenceId: String,
      events: Seq[EventEntry]
  ): Future[Done] = {
    val promisedDone = Promise[Done]()
    persistInMemory.tell(Persist(persistenceId, events, Right(promisedDone)))
    promisedDone.future
  }

  def removeEventsTo(persistenceId: String, to: Long): Future[Done] = {
    val promisedDone = Promise[Done]()
    persistInMemory.tell(RemoveEventsOf(persistenceId, to, Right(promisedDone)))
    promisedDone.future
  }

  def eventsOf(persistenceId: String): Future[Seq[EventWithOffset]] = {
    val promise = Promise[Seq[EventWithOffset]]()
    persistInMemory.tell(EventsOf(persistenceId, Right(promise)))
    promise.future
  }

  def highestSequenceOf(persistenceId: String): Future[Long] = {
    val promise = Promise[Long]()
    persistInMemory.tell(HighestSequenceOf(persistenceId, Right(promise)))
    promise.future
  }

  def addSnapshot(
      persistenceId: String,
      entry: PersistInMemory.SnapshotEntry
  ): Future[Done] = {
    val promisedDone = Promise[Done]()
    persistInMemory.tell(Snapshot(persistenceId, entry, Right(promisedDone)))
    promisedDone.future
  }

  def snapshotsOf(persistenceId: String): Future[Seq[SnapshotEntry]] = {
    val promise = Promise[Seq[SnapshotEntry]]()
    persistInMemory.tell(SnapshotsOf(persistenceId, Right(promise)))
    promise.future
  }

  def removeSnapshotOf(
      persistenceId: String,
      sequences: SequenceRange
  ): Future[Done] = {
    val promisedDone = Promise[Done]()
    persistInMemory.tell(
      RemoveSnapshotsOf(persistenceId, sequences, Right(promisedDone))
    )
    promisedDone.future
  }

  def allEvents(offset: Offset): Future[Source[EventWithOffset, NotUsed]] = {
    val promisedDone = Promise[Source[EventWithOffset, NotUsed]]()
    persistInMemory.tell(AllEvents(offset, Right(promisedDone)))
    promisedDone.future
  }

  def invalidateAll(): Future[Done] = {
    val promisedDone = Promise[Done]()
    persistInMemory.tell(InvalidateAll(Right(promisedDone)))
    promisedDone.future
  }

  def allPersistenceId(): Future[Source[String, NotUsed]] = {
    val promise = Promise[Source[String, NotUsed]]()
    persistInMemory.tell(AllPersistenceIds(Right(promise)))
    promise.future
  }

}
