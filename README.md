# Akka Persistence Plugin for MongoDB
[![CircleCI](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin.svg?style=svg)](https://circleci.com/gh/null-vector/akka-reactivemongo-plugin)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/6bc8194e92dc42b5a536a1a81d982d18)](https://www.codacy.com/app/rodrigogdea/akka-reactivemongo-plugin?utm_source=github.com&utm_medium=referral&utm_content=null-vector/akka-reactivemongo-plugin&utm_campaign=Badge_Coverage)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/6bc8194e92dc42b5a536a1a81d982d18)](https://www.codacy.com/app/rodrigogdea/akka-reactivemongo-plugin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=null-vector/akka-reactivemongo-plugin&amp;utm_campaign=Badge_Grade)

This implementation use the [reactivemongo drive](http://reactivemongo.org/).

## Installation
This plugin needs scala `2.12.x`, akka `2.5.x` and reactivemongo `0.16.x`.

Add in your `build.sbt` the following lines:
```scala
resolvers += "Akka RactiveMongo Plugin" at "https://dl.bintray.com/null-vector/releases"

libraryDependencies += "null-vector" %% "akka-reactivemongo-plugin" % "1.0.3",
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

    private val userAddedMapping: BSONDocumentHandler[UserAdded] = Macros.handler[UserAdded]

    override val manifest: String = "UserAdded"

    override def payloadToBson(payload: UserAdded): BSONDocument = userAddedMapping.write(payload)

    override def bsonToPayload(doc: BSONDocument): UserAdded = userAddedMapping.read(doc)
}
```

And then you have to register the new Adapter:
```scala
  val serializer = ReactiveMongoEventSerializer(system)

  serializer.addEventAdapter(new UserAddedEventAdapter)
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

val readJournal = PersistenceQuery(system).readJournalFor[ReactiveMongoScalaReadJournal](ReactiveMongoJournalProvider.pluginId)

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

