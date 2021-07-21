package org.nullvector.query

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.Materializer
import org.nullvector.ReactiveMongoDriver
import org.nullvector.typed.ReactiveMongoEventSerializer
import reactivemongo.api.bson._

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ReactiveMongoScalaReadJournalImpl(system: ExtendedActorSystem, protected val collectionNames: List[String])
  extends akka.persistence.query.scaladsl.ReadJournal
    with EventsQueries
    with PersistenceIdsQueries with ReactiveMongoScalaReadJournal {

  protected lazy val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(system.toTyped)
  protected lazy val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)
  protected implicit lazy val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-dispatcher")
  protected implicit lazy val materializer: Materializer = Materializer.matFromSystem(system)

  protected val defaultRefreshInterval: FiniteDuration =
    system.settings.config.getDuration("akka-persistence-reactivemongo.read-journal.refresh-interval", TimeUnit.MILLISECONDS).millis

  protected def filterByOffset(offset: Offset): BSONDocument = {
    offset match {
      case ObjectIdOffset(bsonObjectId) => BSONDocument("_id" -> BSONDocument("$gt" -> bsonObjectId))
      case NoOffset | _ => BSONDocument.empty
    }
  }
}
