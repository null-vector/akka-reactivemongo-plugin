lazy val scala212 = "2.12.9"
lazy val scala213 = "2.13.1"
lazy val supportedScalaVersions = List(scala212, scala213)
lazy val akkaVersion = "2.6.1"
lazy val rxmongoVersion = "0.18.8"

name := "akka-reactivemongo-plugin"
organization := "null-vector"
version := "1.2.8"
scalaVersion := scala213
crossScalaVersions := supportedScalaVersions
scalacOptions ++= Seq(
//  "-Xfatal-warnings",  // New lines for each options
  "-deprecation",
  "-feature",
  "-language:implicitConversions",
)
resolvers += "Akka Maven Repository" at "https://akka.io/repository"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "joda-time" % "joda-time" % "2.10.1"

libraryDependencies += "org.reactivemongo" %% "reactivemongo" % rxmongoVersion
libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test

licenses += ("MIT", url("https://opensource.org/licenses/MIT"))

coverageExcludedPackages := "<empty>;.*ReactiveMongoJavaReadJournal.*"

Test / fork := true
Test / javaOptions += "-Xmx4G"
Test / javaOptions += "-XX:+CMSClassUnloadingEnabled"
Test / javaOptions += "-XX:+UseConcMarkSweepGC"
Test / javaOptions += "-Dfile.encoding=UTF-8"
