package org.nullvector.query

import java.util.concurrent.TimeUnit

import akka.actor.ExtendedActorSystem
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.{ActorMaterializer, Materializer}
import org.nullvector.{ReactiveMongoDriver, ReactiveMongoEventSerializer}
import reactivemongo.api.bson._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ReactiveMongoScalaReadJournal(system: ExtendedActorSystem)
  extends akka.persistence.query.scaladsl.ReadJournal
    with EventsQueries
    with PersistenceIdsQueries {

  protected lazy val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(system)
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
