package org.nullvector.crud

import akka.Done
import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{DurableStateBehavior, Effect}
import akka.util.Timeout
import org.nullvector.crud.ReactiveMongoCrud.Schema
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.nullvector.{EventAdapterFactory, ReactiveMongoDriver}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers._
import reactivemongo.api.indexes.IndexType
import reactivemongo.core.errors.DatabaseException

import scala.concurrent.duration.DurationInt
import scala.util.Random

class ReactiveMongoCrudTest extends AsyncFlatSpec {

  implicit private val system: ActorSystem = ActorSystem("Crud")
  val crud                                 = DurableStateStoreRegistry
    .get(system)
    .durableStateStoreFor[ReactiveMongoCrud](ReactiveMongoCrud.pluginId)
  private val driver: ReactiveMongoDriver  = ReactiveMongoDriver(system)

  ReactiveMongoEventSerializer(system.toTyped)
    .addAdapter(EventAdapterFactory.adapt[ChessBoard]("ChessBoard"))

  behavior of "Crud"

  it should "insert an object" in {
    crud
      .upsertObject(randomPersistenceId, 2, ChessBoard(Map("a1" -> "R")), "")
      .map(_ shouldBe Done)
  }

  it should "fail inserting same revision number" in {
    recoverToSucceededIf[DatabaseException] {
      val pid                = randomPersistenceId
      val originalChessBoard = ChessBoard(Map("a1" -> "RB"))
      for {
        _ <- crud.upsertObject(pid, 250, originalChessBoard, "")
        _ <- crud.upsertObject(pid, 250, originalChessBoard, "")
      } yield ()
    }
  }

  it should "update an object" in {
    val pid                = randomPersistenceId
    val originalChessBoard = ChessBoard(Map("a1" -> "RB"))
    val updatedChessBoard  = originalChessBoard.copy(piecePositions = Map("4b" -> "KW"))
    for {
      _       <- crud.upsertObject(pid, 35, originalChessBoard, "")
      result1 <- crud.getObject(pid)
      _       <- crud.upsertObject(pid, 36, updatedChessBoard, "")
      result2 <- crud.getObject(pid)
    } yield {
      result1 shouldBe GetObjectResult(Some(originalChessBoard), 35)
      result2 shouldBe GetObjectResult(Some(updatedChessBoard), 36)
    }
  }

  it should "delete an object" in {
    val pid        = randomPersistenceId
    val chessBoard = ChessBoard(Map("a1" -> "R"))
    for {
      _       <- crud.upsertObject(pid, 35, chessBoard, "")
      result1 <- crud.getObject(pid)
      _       <- crud.deleteObject(pid)
      result2 <- crud.getObject(pid)
    } yield {
      result1 shouldBe GetObjectResult(Some(chessBoard), 35)
      result2 shouldBe GetObjectResult(None, 1)
    }
  }

  it should "load an object" in {
    val pid        = randomPersistenceId
    val chessBoard = ChessBoard(Map("a1" -> "R"))
    for {
      _      <- crud.upsertObject(pid, 35, chessBoard, "")
      result <- crud.getObject(pid)
    } yield result shouldBe GetObjectResult(Some(chessBoard), 35)
  }

  it should "support DurableStoreBehaviour" in {
    implicit val timeout   = Timeout(1.seconds)
    implicit val scheduler = system.toTyped.scheduler
    val testKit            = ActorTestKit(system.toTyped)
    val chessBoardId       = Random.nextLong().abs.toString
    val chessBoardRef      = testKit.spawn(ChessBoardBehavior.behavior(chessBoardId))
    for {
      _        <- chessBoardRef.ask[Done](ref => ChessBoardBehavior.UpdatePosition("b3" -> "QW", ref))
      boardV1  <- chessBoardRef.ask(ChessBoardBehavior.GetBoard(_))
      resultV1 <- crud.getObject(PersistenceId("ChessBoard", chessBoardId).id)
      _        <- chessBoardRef.ask[Done](ref => ChessBoardBehavior.UpdatePosition("e8" -> "RB", ref))
      boardV2  <- chessBoardRef.ask(ChessBoardBehavior.GetBoard(_))
      resultV2 <- crud.getObject(PersistenceId("ChessBoard", chessBoardId).id)
    } yield {
      resultV1 shouldBe GetObjectResult(Some(boardV1), 2L)
      resultV2 shouldBe GetObjectResult(Some(boardV2), 3L)
    }
  }

  it should "have the correct indices" in {
    val aPersistenceId = randomPersistenceId
    for {
      coll    <- driver.crudCollection(aPersistenceId)
      indices <- coll.indexesManager.list()
    } yield {
      val pidIndex     = indices.find(_.name.exists(_ == "persistence_id")).get
      pidIndex.unique shouldBe true
      pidIndex.key shouldBe Seq(Schema.persistenceId -> IndexType.Ascending)
      val versionIndex = indices.find(_.name.exists(_ == "pid_revision")).get
      versionIndex.unique shouldBe true
      versionIndex.key shouldBe Seq(
        Schema.persistenceId -> IndexType.Ascending,
        Schema.revision      -> IndexType.Ascending
      )
    }
  }

  def randomPersistenceId = s"ChessBoard|${Random.nextLong().abs}"

  case class ChessBoard(piecePositions: Map[String, String])

  object ChessBoardBehavior {
    sealed trait Command
    case class UpdatePosition(newPosition: (String, String), reply: ActorRef[Done]) extends Command
    case class GetBoard(reply: ActorRef[ChessBoard])                                extends Command

    def behavior(chessBoardId: String): DurableStateBehavior[Command, ChessBoard] = {

      DurableStateBehavior[Command, ChessBoard](
        PersistenceId("ChessBoard", chessBoardId),
        ChessBoard(Map.empty),
        { (state, command) =>
          command match {
            case GetBoard(reply)                    => Effect.reply(reply)(state)
            case UpdatePosition(newPosition, reply) =>
              Effect
                .persist(state.copy(piecePositions = state.piecePositions + newPosition))
                .thenReply(reply)(_ => Done)
          }
        }
      )
    }
  }
}
