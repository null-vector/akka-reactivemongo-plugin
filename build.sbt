name := "akka-reactivemongo-plugin"
organization := "null-vector"
version := "1.0.0"
scalaVersion := "2.12.6"
val akkaVersion = "2.5.14"
val rxmongoVersion = "0.16.0"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "com.esotericsoftware" % "kryo" % "5.0.0-RC1"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "org.reactivemongo" %% "reactivemongo" % rxmongoVersion
libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test


