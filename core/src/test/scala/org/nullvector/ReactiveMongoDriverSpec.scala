package org.nullvector

import akka.actor.ActorSystem
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.{AsyncDriver, DB}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Try

class ReactiveMongoDriverSpec() extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val system: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)
  val databaseProvider = new DatabaseProvider {
    lazy val db = Try {
      val eventualDb = new AsyncDriver().connect("mongodb://localhost").flatMap(_.database("Provided"))
      Await.result(eventualDb, 10.second)
    }
    override def database = db
  }

  rxDriver.withDatabaseProvider(databaseProvider)

  it should " use a custom database provider" in {
    val eventualResult = rxDriver.journalCollection("Provided-1").flatMap(_.insert(true).one(BSONDocument()))
    Await.result(eventualResult, 6.second).n shouldBe 1
  }

  it should " get server status " in {
    Await.result(rxDriver.serverStatus(), 1.second)
  }

}