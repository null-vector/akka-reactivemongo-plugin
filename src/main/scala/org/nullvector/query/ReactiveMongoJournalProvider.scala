package org.nullvector.query

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.PersistentRepr
import akka.persistence.query._
import akka.stream.{ActorMaterializer, Materializer, javadsl}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import org.nullvector.{Fields, ReactiveMongoDriver, ReactiveMongoEventSerializer}
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONObjectID}

object ReactiveMongoJournalProvider {
  val pluginId = "akka-persistence-reactivemongo.read-journal"
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    new ReactiveMongoScalaReadJournal(system, config)

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}



