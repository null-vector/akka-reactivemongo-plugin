# Akka Persistence Plugin for MongoDB
[![codecov](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin/branch/master/graph/badge.svg)](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin)

This implementation uses the [ReactiveMongo driver](http://reactivemongo.org/).

## Installation
This plugin supports Scala `2.13` and Scala `3.3`, Akka Persistence (`2.9.x` on 2.13, `2.8.x` on 3), and ReactiveMongo `1.1.x`.

Add in your `build.sbt` the following lines:
```scala
resolvers += "null-vector" at "https://nullvector.jfrog.io/artifactory/releases"
```
```scala
libraryDependencies += "null-vector" %% "akka-reactivemongo-plugin" % "1.6.10"
```

Examples below use Scala 3 style (`given`). On Scala 2.13 the same API works with `implicit val` (this project also enables `-Xsource:3`).

## Configuration
To activate the plugin and set the MongoDB URI, add to your `application.conf`:
```
akka.persistence.journal.plugin = "akka-persistence-reactivemongo-journal"
akka.persistence.snapshot-store.plugin = "akka-persistence-reactivemongo-snapshot"

akka-persistence-reactivemongo {
  mongo-uri = "mongodb://host/database?rm.failover=900ms:21x1.30"
}
```
See [Connect to a database](http://reactivemongo.org/releases/0.1x/documentation/tutorial/connect-database.html) for more information.
### Configuration for DurableStateStore
```
akka.persistence.state.plugin = "akka-persistence-reactivemongo-crud"
```
## Events Adapters
Before saving any event from your persistent actor you need to register the corresponding `EventAdapter`.
```scala
case class ProductId(id: String) extends AnyVal
case class InvoiceItem(productId: ProductId, price: BigDecimal, tax: BigDecimal)
case class InvoiceItemAdded(invoiceItem: InvoiceItem)

val eventAdapter = EventAdapterFactory.adapt[InvoiceItemAdded]("InvoiceItemAdded")

ReactiveMongoEventSerializer(actorSystem).addEventAdapter(eventAdapter)
```
It is also possible to override mappings or add unsupported mappings. Added mappings must extend `BSONReader[_]`, `BSONWriter[_]`, or both:
```scala
given BSONReader[Type1] = new BSONReader[Type1] { ... }
given BSONWriter[Type2] = new BSONWriter[Type2] { ... }
given (BSONReader[Type3] & BSONWriter[Type3]) = new BSONReader[Type3] with BSONWriter[Type3] { ... }

val eventAdapter = EventAdapterFactory.adapt[Type4]("SomeEvent")
```
You can also add tags associated with the event:
```scala
val eventAdapter = EventAdapterFactory.adapt[Type4]("SomeEvent", Set("Tag_1", "Tag_2"))
```
Sealed trait families (sum types) are mapped automatically:
```scala
sealed trait InvoiceLineType
case object ProductLine extends InvoiceLineType
// ...
case class InvoiceLine(lineType: InvoiceLineType, ...)
case class InvoiceLineAdded(line: InvoiceLine)
// ...
given MacroConfiguration.Aux[MacroOptions] =
  MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)

val eventAdapter = EventAdapterFactory.adapt[InvoiceLineAdded]("InvoiceLineAdded")
```
`EventAdapterFactory` derives BSON document mappings automatically (Mirror-based on Scala 3; ReactiveMongo macros on Scala 2). Configure sealed-family discriminators with:
```scala
given MacroConfiguration.Aux[MacroOptions] =
  MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
```
### Custom mappings
You can create mappings by hand:
```scala
given BSONDocumentMapping[SolarPlanet] = EventAdapterFactory.mappingOf[SolarPlanet]
val eventAdapter = new EventAdapterMapping[SolarPlanet]("planet")

serializer.addEventAdapter(eventAdapter)
```

## Persistence Id
By default, the persistence id has the following form: `<Aggregate>-<Id>`, and the aggregate will be the name of the MongoDB collection.

You can change the persistence id separator character:
```
akka-persistence-reactivemongo {
  mongo-uri = "mongodb://localhost/test?rm.failover=900ms:21x1.30"
  persistence-id-separator = |
}
```

## Persistence Query

Here are some examples of how to use persistence query:
```scala
val readJournal = ReactiveMongoJournalProvider(system).scaladslReadJournal
val tagsSource: Source[EventEnvelope, NotUsed] = readJournal.currentEventsByTag("some_tag", NoOffset)

tagsSource.runWith(Sink.foreach { envelope =>
  envelope.event match {
    case UserAdded(name, age) => // Do Something
  }
})
```

Sometimes it is necessary to create an Offset:
```scala
val offset = ObjectIdOffset.fromDateTime(DateTime.now()) // A Joda DateTime
```
For streams that never complete like `#persistenceIds`, `#eventsByTag`, etc. it is possible to configure the interval that pulls from the journal:
```
akka-persistence-reactivemongo {
  mongo-uri = "mongodb://localhost/test?rm.failover=900ms:21x1.30"
  read-journal {
    refresh-interval = 2s
  }
}
```
If you want different refresh intervals for different queries, add a `RefreshInterval` attribute on the Source:
```scala
readJournal
  .eventsByTag("some_tag", NoOffset)
  .addAttributes(RefreshInterval(700.millis))
  .runWith(Sink.foreach(println))
```
## Filter Events by some Event's Attribute
### From regular stream
```scala
val readJournal = ReactiveMongoJournalProvider(system).readJournalFor(Seq("Orders"))
readJournal
  .currentEventsByTags(Seq("TAG"), NoOffset, BSONDocument("events.p.customerId" -> customerId), None)
  .mapAsync(envelope => someEventualWork(envelope))
  .run()
```
### From non-termination stream
```scala
val readJournal = ReactiveMongoJournalProvider(system).readJournalFor(Seq("Orders"))
readJournal
  .eventsByTags(Seq("TAG"), NoOffset, BSONDocument("events.p.customerId" -> customerId), None, 5.seconds)
  .mapAsync(envelope => someEventualWork(envelope))
  .run()
```
# Test Driven Development
Here is a great feature for TDD lovers: it is possible to configure the plugin to persist in memory and reduce the test latency more than half.
```
akka-persistence-reactivemongo {
  persist-in-memory = true
}
```
