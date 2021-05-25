package org.nullvector.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.PersistentRepr
import org.nullvector.EventAdapterFactory
import org.nullvector.typed.ReactiveMongoEventSerializer.Registry
import org.scalatest.Matchers.{a, convertToAnyShouldWrapper, thrownBy}
import org.scalatest._
import reactivemongo.api.bson.BSONDocument

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class EventAdapterSerializerSpec extends FlatSpec {

  private val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "TypedSerializer")

  it should " adapter not found " in {
    val repr = PersistentRepr(OneEvent("One"), manifest = "OneManifest")
    val future = ReactiveMongoEventSerializer(system).deserialize(Seq(repr))
    a[Registry#EventAdapterNotFound] shouldBe thrownBy(Await.result(future, 1.second))
  }

  it should " deserialize " in {
    val serializer = ReactiveMongoEventSerializer(system)
    val repr = PersistentRepr(BSONDocument("name" -> "AName"), manifest = "OneManifest")
    serializer.addAdapters(Seq(EventAdapterFactory.adapt[OneEvent]("OneManifest")))
    val future = serializer.deserialize(Seq(repr))
    Await.result(future, 1.second).head.payload shouldBe OneEvent("AName")
  }

  it should " serialize " in {
    val serializer = ReactiveMongoEventSerializer(system)
    val repr = PersistentRepr(TwoEvent("TwoEventName"), manifest = "TwoManifest")
    serializer.addAdapters(Seq(EventAdapterFactory.adapt[TwoEvent]("TwoManifest", Set("TwoEventTag"))))
    val future = serializer.serialize(Seq(repr))
    val deserialized = Await.result(future, 1.second)
    deserialized.head._1.payload shouldBe BSONDocument("name" -> "TwoEventName")
    deserialized.head._2 shouldBe Set("TwoEventTag")
  }


  case class OneEvent(name: String)

  case class TwoEvent(name: String)
}


