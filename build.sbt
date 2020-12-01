lazy val scala213 = "2.13.4"
lazy val supportedScalaVersions = List(scala213)
lazy val akkaVersion = "2.6.10"
lazy val rxmongoVersion = "1.0.1"

lazy val commonSettings = Seq(
  name := "akka-reactivemongo-plugin",
  organization := "null-vector",
  version := "1.4.2",
  scalaVersion := scala213,
  crossScalaVersions := supportedScalaVersions,
  scalacOptions := Seq(
    "-encoding", "UTF-8", "-target:jvm-1.8", "-deprecation",
    "-language:experimental.macros",
    //    "-Ymacro-annotations",
    "-feature",
    "-unchecked",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-Ybackend-parallelism",
    "4",
  ),
  resolvers += "Akka Maven Repository" at "https://akka.io/repository",

  libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,

  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",

  libraryDependencies += "joda-time" % "joda-time" % "2.10.1",

  libraryDependencies += "org.reactivemongo" %% "reactivemongo" % rxmongoVersion,
  libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoVersion,

  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  //libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  libraryDependencies += "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,

  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),

  coverageExcludedPackages := "<empty>;.*ReactiveMongoJavaReadJournal.*",

  Test / fork := true,
  Test / javaOptions += "-Xmx4G",
  Test / javaOptions += "-XX:+CMSClassUnloadingEnabled",
  Test / javaOptions += "-Dfile.encoding=UTF-8",
)

lazy val core = (project in file("core"))
  .dependsOn(
    macros,
    api)
  .settings(
    commonSettings,
    Compile / packageDoc / publishArtifact := false,
    Compile / packageBin / mappings ++= (macros / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (macros / Compile / packageSrc / mappings).value,
    Compile / packageBin / mappings ++= (api / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (api / Compile / packageSrc / mappings).value,
  )

lazy val macros = (project in file("macros"))
  .dependsOn(api)
  .settings(
    commonSettings,
    publish := {},
    publishLocal := {}
  )

lazy val api = (project in file("api"))
  .settings(
    commonSettings,
    publish := {},
    publishLocal := {}
  )