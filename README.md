# Akka Persistence Plugin for MongoDB
[![CircleCI](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin.svg?style=svg)](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin)
[![codecov](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin/branch/master/graph/badge.svg)](https://codecov.io/gh/null-vector/akka-reactivemongo-plugin)

This implementation use the [reactivemongo drive](http://reactivemongo.org/).

## Installation
This plugin support scala `2.12` and `2.13`, akka `2.6.1` and reactivemongo `0.18.x` and `0.19.x`.

Add in your `build.sbt` the following lines:
```scala
resolvers += Resolver.bintrayRepo("null-vector", "releases")
```
For reactivemongo `0.18.x` use:

[![Download](https://api.bintray.com/packages/null-vector/releases/akka-reactivemongo-plugin/images/download.svg?version=1.2.11) ](https://bintray.com/null-vector/releases/akka-reactivemongo-plugin/1.2.11/link)

```scala
libraryDependencies += "null-vector" %% "akka-reactivemongo-plugin" % "1.2.x"
```
For reactivemongo `0.19.x` use:

[ ![Download](https://api.bintray.com/packages/null-vector/releases/akka-reactivemongo-plugin/images/download.svg) ](https://bintray.com/null-vector/releases/akka-reactivemongo-plugin/_latestVersion)

```scala
libraryDependencies += "null-vector" %% "akka-reactivemongo-plugin" % "1.3.x"
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
Before save any event for you `PersistentActor` it is needed to add the corresponding `EventAdapter`.

Events adapters must extends from `org.nullvector.EventAdapter[E]`, for example:

```scala
class UserAddedEventAdapter extends EventAdapter[UserAdded] {

    private implicit val userAddedMapping: BSONDocumentHandler[UserAdded] = Macros.handler[UserAdded]

    override val manifest: String = "UserAdded"

    override def payloadToBson(payload: UserAdded): BSONDocument = BSON.writeDocument(payload).get

    override def bsonToPayload(doc: BSONDocument): UserAdded = BSON.readDocument(doc).get

}
```
And then you have to register the new Adapter:
```scala
  val serializer = ReactiveMongoEventSerializer(system)

  serializer.addEventAdapter(new UserAddedEventAdapter)
```
A more simple way to create an event adapter by hand is using ``EventAdapterMapping`:
```scala
    implicit val mapping: BSONDocumentMapping[SolarPlanet] = EventAdapterFactory.mappingOf[SolarPlanet]
    val eventAdapter = new EventAdapterMapping[SolarPlanet](manifest = "planet")
```
## EventAdapter Factory
To avoid writing boilerplate code creating Event Adapters, we can use the `EventAdapterFactory`:
```scala
case class ProductId(id: String) extends AnyVal
case class InvoiceItem(productId: ProductId, price: BigDecimal, tax: BigDecimal)
case class InvoiceItemAdded(invoiceItem: InvoiceItem)

val eventAdapter = EventAdapterFactory.adapt[InviceItemAdded](withManifest = "InvoceItemAdded")

ReactiveMongoEventSerializer(ActorSystem()).addEventAdapter(eventAdapter)
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

## Persistence Id
By default the persistence id has the following form: `<Aggregate>-<Id>`, and the aggregate will be the name of the journal collection.

You can change the persistence id format by adding your own collection extractor name, implementing the trait `org.nullvector.CollectionNameMapping`,
and registering in the configuration:
```
akka-persistence-reactivemongo {
  mongo-uri = "mongodb://localhost/test?rm.failover=900ms:21x1.30"
  collection-name-mapping = "org.nullvector.DefaultCollectionNameMapping"
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

Sometime is necesary to create an Offset:
```scala
val offset = ObjectIdOffset(DateTime.now())
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
