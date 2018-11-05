package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class EventsByIdSource(
                         persistenceId: String,
                         fromSequenceNr: Long,
                         toSequenceNr: Long,
                         refreshInterval: FiniteDuration,
                         currentById: (String, Long, Long) => Source[EventEnvelope, NotUsed]
                       )(implicit ec: ExecutionContext) extends GraphStage[SourceShape[Source[EventEnvelope, NotUsed]]] {

  private val outlet: Outlet[Source[EventEnvelope, NotUsed]] = Outlet[Source[EventEnvelope, NotUsed]]("EventsByTags.OUT")

  override def shape: SourceShape[Source[EventEnvelope, NotUsed]] = SourceShape.of(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    var currentFromSequenceNr: Long = fromSequenceNr
    var currentToSequenceNr: Long = toSequenceNr
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
        push(outlet, currentById(persistenceId, currentFromSequenceNr, currentToSequenceNr).map { env =>
          currentFromSequenceNr = env.sequenceNr
          currentToSequenceNr = Long.MaxValue
          env
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
