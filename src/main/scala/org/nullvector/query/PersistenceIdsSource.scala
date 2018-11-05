package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.nullvector.query.PersistenceIdsQueries.PersistenceId

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class PersistenceIdsSource(
                            offset: Offset,
                            refreshInterval: FiniteDuration,
                            currentIds: Offset => Source[PersistenceId, NotUsed]
                          )(implicit ec: ExecutionContext) extends GraphStage[SourceShape[Source[PersistenceId, NotUsed]]] {

  private val outlet: Outlet[Source[PersistenceId, NotUsed]] = Outlet[Source[PersistenceId, NotUsed]]("EventsByTags.OUT")

  override def shape: SourceShape[Source[PersistenceId, NotUsed]] = SourceShape.of(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    var currentOffset: Offset = offset
    var eventStreamConsuming = false

    setHandler(outlet, new OutHandler {
      override def onPull(): Unit = {}

      override def onDownstreamFinish(): Unit = {
        cancelTimer("timer")
      }

    })

    override def preStart(): Unit = schedulePeriodicallyWithInitialDelay("timer", refreshInterval, refreshInterval)

    override protected def onTimer(timerKey: Any): Unit = {
      if (isAvailable(outlet) && !eventStreamConsuming) {
        eventStreamConsuming = true
        push(outlet, currentIds(currentOffset).map { persistenceId =>
          (currentOffset, persistenceId.offset) match {
            case (NoOffset, _) => currentOffset = persistenceId.offset
            case (_currentOffset: ObjectIdOffset, eventOffset: ObjectIdOffset) if _currentOffset < eventOffset =>
              currentOffset = persistenceId.offset
            case _ =>
          }
          persistenceId
        }
          .watchTermination() { (_, future) =>
            future.onComplete { _ => eventStreamConsuming = false }
            NotUsed
          }
        )
      }
    }

  }

}
