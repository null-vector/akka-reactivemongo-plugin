package org.nullvector.crud

import akka.Done
import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.query.NoOffset
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{DurableStateBehavior, Effect}
import akka.util.Timeout
import org.nullvector.crud.ReactiveMongoCrud.{CrudOffset, Schema}
import org.nullvector.typed.ReactiveMongoEventSerializer
import org.nullvector.{EventAdapterFactory, ReactiveMongoDriver}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers.*
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.indexes.IndexType
import reactivemongo.core.errors.DatabaseException

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.util.Random

class ReactiveMongoCrudTest extends AsyncFlatSpec {

  implicit private val system: ActorSystem = ActorSystem("Crud")
  val crud                                 = DurableStateStoreRegistry(system)
    .durableStateStoreFor[ReactiveMongoCrud[ChessBoard]](ReactiveMongoCrud.pluginId)
  private val driver: ReactiveMongoDriver  = ReactiveMongoDriver(system)
  val testKit                              = ActorTestKit(system.toTyped)

  ReactiveMongoEventSerializer(system.toTyped)
    .addAdapter(EventAdapterFactory.adapt[ChessBoard]("ChessBoard"))

  behavior of "Crud"

  it should "insert an object" in {
    crud
      .upsertObject(randomPersistenceId, 1, ChessBoard(Map("a1" -> "R")), "")
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
      _       <- crud.upsertObject(pid, 1, originalChessBoard, "")
      result1 <- crud.getObject(pid)
      _       <- crud.upsertObject(pid, 2, updatedChessBoard, "")
      result2 <- crud.getObject(pid)
    } yield {
      result1 shouldBe GetObjectResult(Some(originalChessBoard), 1)
      result2 shouldBe GetObjectResult(Some(updatedChessBoard), 2)
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
      result2 shouldBe GetObjectResult(None, 0)
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
      resultV1 shouldBe GetObjectResult(Some(boardV1), 1L)
      resultV2 shouldBe GetObjectResult(Some(boardV2), 2L)
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

  it should "Query current updates on ChessBoard" in {
    val offset        = CrudOffset(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).toInstant)
    val persistenceId = randomPersistenceId
    for {
      _            <- crud.upsertObject(persistenceId, 1, ChessBoard(Map("a1" -> "R")), "")
      updatedCount <- crud.query("ChessBoard", 2.seconds).currentChanges("", offset).runFold(0) {
                        case (counter, updated) if updated.persistenceId == persistenceId => counter + 1
                        case (counter, _)                                                 => counter
                      }
      docCount     <- driver.crudCollectionOfEntity("ChessBoard").flatMap(_.count(Some(BSONDocument(Schema.persistenceId -> persistenceId))))
    } yield {
      updatedCount shouldBe docCount
    }
  }

  it should "Query updates on ChessBoard periodically" in {
    val atomicInteger        = new AtomicInteger()
    crud
      .query("ChessBoard", 100.millis)
      .changes("", NoOffset)
      .map(_ => atomicInteger.incrementAndGet())
      .run()
    Thread.sleep(1000)
    val currentUpdatesBefore = atomicInteger.get()

    implicit val timeout   = Timeout(1.seconds)
    implicit val scheduler = system.toTyped.scheduler
    val chessBoardId       = Random.nextLong().abs.toString
    val chessBoardRef      = testKit.spawn(ChessBoardBehavior.behavior(chessBoardId))
    for {
      _                  <- chessBoardRef.ask[Done](ref => ChessBoardBehavior.UpdatePosition("b3" -> "QW", ref))
      _                   = Thread.sleep(500)
      currentUpdatesAfter = atomicInteger.get()
    } yield {
      currentUpdatesAfter shouldBe (currentUpdatesBefore + 1)
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
