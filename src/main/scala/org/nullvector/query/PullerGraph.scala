package org.nullvector.query

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}


import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class PullerGraph[D, O](
                         initialOffset: O,
                         refreshInterval: FiniteDuration,
                         offsetOf: D => O,
                         graterOf: (O, O) => O,
                         nextChunk: O => Source[D, NotUsed],
                       )(implicit ec: ExecutionContext) extends GraphStage[SourceShape[Source[D, NotUsed]]] {

  private val outlet: Outlet[Source[D, NotUsed]] = Outlet[Source[D, NotUsed]]("PullerGraph.OUT")

  override def shape: SourceShape[Source[D, NotUsed]] = SourceShape.of(outlet)

  override def createLogic(attributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    private val effectiveRefreshInterval: FiniteDuration = attributes.get[RefreshInterval].map(_.interval).getOrElse(refreshInterval)
    var currentOffset: O = initialOffset
    var eventStreamConsuming = false

    private val updateConsumingState = createAsyncCallback[Boolean](consumingState => eventStreamConsuming = consumingState)

    setHandler(outlet, new OutHandler {
      override def onPull(): Unit = {}

      override def onDownstreamFinish(cause: Throwable): Unit = cancelTimer("timer")
    })

    override def preStart(): Unit = scheduleWithFixedDelay("timer", effectiveRefreshInterval, effectiveRefreshInterval)

    override protected def onTimer(timerKey: Any): Unit = {
      if (isAvailable(outlet) && !eventStreamConsuming) {
        eventStreamConsuming = true
        push(outlet, nextChunk(currentOffset).map { entry =>
          currentOffset = graterOf(currentOffset, offsetOf(entry))
          entry
        }
          .watchTermination() { (_, future) =>
            future.onComplete { _ => updateConsumingState.invoke(false) }
            NotUsed
          }
        )
      }
    }

  }

}
