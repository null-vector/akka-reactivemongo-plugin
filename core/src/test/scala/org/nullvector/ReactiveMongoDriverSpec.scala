package org.nullvector

import akka.actor.ActorSystem
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.{AsyncDriver, DB}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class ReactiveMongoDriverSpec() extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val system: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  protected val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)

  it should " use a custom database provider" in {
    val databaseProvider = new DatabaseProvider {
      lazy val db = {
        val eventualDb = new AsyncDriver().connect("mongodb://localhost").flatMap(_.database("Provided"))
        Await.result(eventualDb, 15.second)
      }

      override def database: DB = db
    }

    rxDriver.withDatabaseProvider(databaseProvider)

    val eventualResult = rxDriver.journalCollection("provided-1").flatMap(_.insert(true).one(BSONDocument()))
    Await.result(eventualResult, 5.second).n shouldBe 1
  }


}