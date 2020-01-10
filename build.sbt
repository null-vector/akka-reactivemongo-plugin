lazy val scala212 = "2.12.9"
lazy val scala213 = "2.13.1"
lazy val supportedScalaVersions = List(scala212, scala213)
lazy val akkaVersion = "2.5.26"
lazy val rxmongoVersion = "0.18.7"

name := "akka-reactivemongo-plugin"
organization := "null-vector"
version := "1.0-SNAPSHOT"
scalaVersion := scala213
crossScalaVersions := supportedScalaVersions

resolvers += "Akka Maven Repository" at "http://akka.io/repository"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "joda-time" % "joda-time" % "2.10.1"

libraryDependencies += "org.reactivemongo" %% "reactivemongo" % rxmongoVersion
libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

coverageExcludedPackages := "<empty>;.*ReactiveMongoJavaReadJournal.*"
