package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class EventsByTagsSource(
                          tags: Seq[String],
                          offset: Offset,
                          refreshInterval: FiniteDuration,
                          currentByTags: (Seq[String], Offset) => Source[EventEnvelope, NotUsed]
                        )(implicit ec: ExecutionContext) extends GraphStage[SourceShape[Source[EventEnvelope, NotUsed]]] {

  private val outlet: Outlet[Source[EventEnvelope, NotUsed]] = Outlet[Source[EventEnvelope, NotUsed]]("EventsByTags.OUT")

  override def shape: SourceShape[Source[EventEnvelope, NotUsed]] = SourceShape.of(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    var currentOffset: Offset = offset
    var eventStreamConsuming = false

    setHandler(outlet, new OutHandler {
      override def onPull(): Unit = {}
    })

    override def preStart(): Unit = schedulePeriodicallyWithInitialDelay("poller-timer", refreshInterval, refreshInterval)

    override protected def onTimer(timerKey: Any): Unit = {
      if (isAvailable(outlet) && !eventStreamConsuming) {
        eventStreamConsuming = true
        push(outlet, currentByTags(tags, currentOffset).map { env =>
          (currentOffset, env.offset) match {
            case (NoOffset, _) => currentOffset = env.offset
            case (_currentOffset: ObjectIdOffset, eventOffset: ObjectIdOffset) if _currentOffset < eventOffset => currentOffset = env.offset
            case _ =>
          }
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
