package org.nullvector

import akka.Done
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.nullvector.ReactiveMongoDriver.DatabaseProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.indexes.IndexType.Ascending
import reactivemongo.api.{AsyncDriver, DB}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.{Failure, Try}

class ReactiveMongoDriverSpec() extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  private val system: ActorSystem                   = ActorSystem()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  protected val rxDriver: ReactiveMongoDriver       = ReactiveMongoDriver(system)
  val databaseProvider                              = new DatabaseProvider {
    lazy val db = Try {
      val eventualDb = new AsyncDriver()
        .connect("mongodb://localhost")
        .flatMap(_.database("Provided"))
      Await.result(eventualDb, 10.second)
    }

    override def database = db
  }

  rxDriver.withDatabaseProvider(databaseProvider)

  behavior of "Reactive Mongo Drive"

  it should " use a custom database provider" in {
    rxDriver
      .journalCollection("Provided-1")
      .flatMap(_.insert(true).one(BSONDocument()))
      .map(_.n shouldBe 1)
  }

  it should " check health " in {
    rxDriver.health().map(_ shouldBe Done)
  }

  it should " Persistent Id Mapping " in {
    val config      = ConfigFactory.parseString("akka-persistence-reactivemongo.persistence-id-separator = |")
    val nameMapping = new DefaultCollectionNameMapping(config)

    nameMapping.collectionNameOf("Entity|") shouldBe Some("Entity")
    nameMapping.collectionNameOf("Entity|1") shouldBe Some("Entity")
    nameMapping.collectionNameOf("Entity") shouldBe None
  }

  it should " get collection by entity name " in {
    rxDriver
      .crudCollectionOfEntity("AnEntity")
      .map(_.name shouldBe "crud_AnEntity")
  }

  it should " ensure crud indices " in {
    for {
      collection   <- rxDriver.crudCollectionOfEntity("AnEntity22")
      indices      <- collection.indexesManager.list()
      namesWithKeys = indices.flatMap(index => index.name.filterNot(_ == "_id_").map((_, index.key.toList, index.unique)))
    } yield namesWithKeys should contain.theSameElementsAs(
      List(
        ("pid_revision", List("pid" -> Ascending, "revision" -> Ascending), true),
        ("updated_tags", List("updated" -> Ascending, "tags" -> Ascending), false),
        ("updated", List("updated" -> Ascending), false),
        ("persistence_id", List("pid" -> Ascending), true)
      )
    )
  }

  it should " check health fail " in {
    rxDriver.withDatabaseProvider(new DatabaseProvider {
      override def database: Try[DB] = Failure(new Exception("BOM"))
    })
    an[Exception] shouldBe thrownBy(Await.result(rxDriver.health(), 1.second))
  }

}
