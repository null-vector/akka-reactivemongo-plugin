package org.nullvector.query

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PullerGraph[Element, Offset](
    initialOffset: Offset,
    refreshInterval: FiniteDuration,
    offsetOf: Element => Offset,
    greaterOf: (Offset, Offset) => Offset,
    query: Offset => Source[Element, NotUsed]
)(implicit ec: ExecutionContext, mat: Materializer)
    extends GraphStage[SourceShape[Seq[Element]]] {

  private val outlet: Outlet[Seq[Element]] =
    Outlet[Seq[Element]]("PullerGraph.OUT")

  override def shape: SourceShape[Seq[Element]] = SourceShape.of(outlet)

  override def createLogic(attributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      var currentOffset: Offset                            = initialOffset
      private val effectiveRefreshInterval: FiniteDuration =
        attributes.get[RefreshInterval].fold(refreshInterval)(_.interval)
      private val updateCurrentOffset                      =
        createAsyncCallback[Offset](offset => currentOffset = offset)
      private val failAsync                                =
        createAsyncCallback[Throwable](throwable => failStage(throwable))
      private val pushElements                             =
        createAsyncCallback[Seq[Element]](elements => push(outlet, elements))

      private val timerName = "timer"
      setHandler(
        outlet,
        new OutHandler {
          override def onPull() = scheduleNext()

          override def onDownstreamFinish(cause: Throwable) =
            cancelTimer(timerName)
        }
      )

      override protected def onTimer(timerKey: Any) = {
        query(currentOffset)
          .runFold(new Accumulator(currentOffset))((acc, element) => acc.update(element))
          .flatMap(_.pushOrScheduleNext())
          .recover { case throwable: Throwable => failAsync.invoke(throwable) }
      }

      private def scheduleNext() = {
        if (!isTimerActive(timerName)) {
          scheduleOnce(timerName, effectiveRefreshInterval)
        }
      }

      class Accumulator(
          private var latestOffset: Offset,
          private val elements: mutable.Buffer[Element] = mutable.Buffer.empty
      ) {

        def update(anElement: Element): Accumulator = {
          latestOffset = greaterOf(latestOffset, offsetOf(anElement))
          elements.append(anElement)
          this
        }

        def pushOrScheduleNext(): Future[Unit] = {
          if (elements.nonEmpty) {
            for {
              _ <- updateCurrentOffset.invokeWithFeedback(latestOffset)
              _ <- pushElements.invokeWithFeedback(elements.toSeq)
            } yield ()
          } else Future.successful(scheduleNext())
        }
      }

    }
}
