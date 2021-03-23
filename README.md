# Akka Persistence Plugin for MongoDB
[![CircleCI](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin.svg?style=svg)](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin)
[![codecov](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin/branch/master/graph/badge.svg)](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin)

This implementation use the [reactivemongo drive](http://reactivemongo.org/).

## Installation
This plugin support scala `2.13`, akka `2.6.x` and reactivemongo `1.0.x`.

Add in your `build.sbt` the following lines:
```scala
resolvers += "null-vector" at "https://nullvector.jfrog.io/artifactory/releases"
```
```scala
libraryDependencies += "null-vector" %% "akka-reactivemongo-plugin" % "1.4.8"
```

## Configuration
To active the plugin an set the mongodb uri you have to add in your application.conf the following lines:
```
akka.persistence.journal.plugin = "akka-persistence-reactivemongo-journal"
akka.persistence.snapshot-store.plugin = "akka-persistence-reactivemongo-snapshot"

akka-persistence-reactivemongo {
  mongo-uri = "mongodb://host/database?rm.failover=900ms:21x1.30"
}
```
See [Connect to a database](http://reactivemongo.org/releases/0.1x/documentation/tutorial/connect-database.html) for more information.

## Events Adapters
Before save any event from your persistent actor it is needed to register the corresponding `EventAdapter`.
```scala
case class ProductId(id: String) extends AnyVal
case class InvoiceItem(productId: ProductId, price: BigDecimal, tax: BigDecimal)
case class InvoiceItemAdded(invoiceItem: InvoiceItem)

val eventAdapter = EventAdapterFactory.adapt[InviceItemAdded](withManifest = "InvoceItemAdded")

ReactiveMongoEventSerializer(actorSystem).addEventAdapter(eventAdapter)
```
It is also possible to override mappings or add unsupported mappings. All added mappings must extends from `BSONReader[_]` or `BSONWriter[_]` or both.
```scala
implicit val reader = new BSONReader[Type1] {...}
implicit val writer = new BSONWriter[Type2] {...}
implicit val readerAndWriter = new BSONReader[Type3] with BSONWriter[Type3] {...}

val eventAdapter = EventAdapterFactory.adapt[Type4](withManifest = "SomeEvent")
```
You can also add tags asociated to the Event:
```scala
val eventAdapter = EventAdapterFactory.adapt[Type4](withManifest = "SomeEvent", Set("Tag_1", "Tag_2"))
```
Traits famlily (`sealed trait`), aka: sum types, are mapped automatically:
```scala
sealed trait InvoiceLineType
case object ProductLine extends InvoiceLineType
...
case class InvoiceLine(lineType: InvoiceLineType, ...)
case class InvoiceLineAdded(line: InvoiceLine)
...
implicit val conf = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
val eventAdapter = EventAdapterFactory.adapt[InvoceLineAdded](withManifest = "InvoiceLineAdded")
```
Behind the scene `EventAdapterFactory` use the ReactiveMongo Macros, so you can configure the BSON mappings:
```scala
implicit val conf: Aux[MacroOptions] = MacroConfiguration(discriminator = "_type", typeNaming = TypeNaming.SimpleName)
```
### Custom mappings
You can create mappings by hand:
```scala
implicit val a: BSONDocumentMapping[SolarPlanet] = EventAdapterFactory.mappingOf[SolarPlanet]
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

tagsSource.runWith(Sink.foreach{ envelope => envelope.event match {
  case UserAdded(name, age) => // Do Something
}})
```

Sometime is necessary to create an Offset:
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
If you want different refresh intervals from different query, you can add a `RefreshInterval` Attribute in the Source definition:
```scala
  readJournal
    .eventsByTag("some_tag", NoOffset)
    .addAttributes(RefreshInterval(700.millis))
    .runWith(Sink.foreach(println))
```

# Test Driven Development
Here is a great feature for TDD lovers: it is possible to configure the plugin to persist in memory and reduce the test latency more than half.
```
akka-persistence-reactivemongo {
  persist-in-memory = true
}
```
