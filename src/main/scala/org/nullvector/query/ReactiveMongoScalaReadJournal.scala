package org.nullvector.query

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import org.nullvector.{Fields, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.reactivestreams.Subscriber
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}
import reactivemongo.akkastream.cursorProducer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ReactiveMongoScalaReadJournal(system: ExtendedActorSystem)
  extends akka.persistence.query.scaladsl.ReadJournal
    with EventsQueries
    with PersistenceIdsQueries {

  protected lazy val serializer: ReactiveMongoEventSerializer = ReactiveMongoEventSerializer(system)
  protected lazy val rxDriver: ReactiveMongoDriver = ReactiveMongoDriver(system)
  protected implicit lazy val dispatcher: ExecutionContext = system.dispatchers.lookup("akka-persistence-reactivemongo-journal-dispatcher")
  protected implicit lazy val materializer: Materializer = ActorMaterializer()(system)

  protected val defaultRefreshInterval: FiniteDuration =
    system.settings.config.getDuration("akka-persistence-reactivemongo.read-journal.refresh-interval", TimeUnit.MILLISECONDS).millis

  protected def filterByOffset(offset: Offset): BSONDocument = {
    offset match {
      case ObjectIdOffset(bsonObjectId) => BSONDocument("_id" -> BSONDocument("$gt" -> bsonObjectId))
      case NoOffset | _ => BSONDocument.empty
    }
  }


}
