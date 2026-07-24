lazy val scala213               = "2.13.11"
lazy val scala3                 = "3.3.0"
lazy val supportedScalaVersions = List(scala213, scala3)
// Akka 2.9.x Scala 3 artifacts require the licensed Akka repo; use 2.8.5 for Scala 3.
lazy val akkaVersion213         = "2.9.1"
lazy val akkaVersion3           = "2.8.5"
lazy val rxmongoVersion         = "1.1.0-RC20"
lazy val rxmongoStreamVersion   = "1.1.0-RC20"

ThisBuild / scalaVersion       := scala213
ThisBuild / version            := s"1.6.10"
ThisBuild / crossScalaVersions := supportedScalaVersions

publishArtifact := false
publish         := {}
publishLocal    := {}

def scalacOptionsFor(scalaV: String): Seq[String] =
  CrossVersion.partialVersion(scalaV) match {
    case Some((2, _)) =>
      Seq(
        "-Xsource:3",
        "-release:17",
        "-deprecation",
        "-language:experimental.macros",
        "-feature",
        "-unchecked",
        "-Werror",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-Ybackend-parallelism",
        "6"
      )
    case Some((3, _)) =>
      Seq(
        "-release:17",
        "-deprecation",
        "-feature",
        "-unchecked",
        "-Werror",
        "-language:implicitConversions"
      )
    case _ => Seq.empty
  }

def akkaVersionFor(scalaV: String): String =
  CrossVersion.partialVersion(scalaV) match {
    case Some((3, _)) => akkaVersion3
    case _            => akkaVersion213
  }

lazy val commonSettings = Seq(
  name                                       := "akka-reactivemongo-plugin",
  organization                               := "null-vector",
  crossScalaVersions                         := supportedScalaVersions,
  scalacOptions                              := scalacOptionsFor(scalaVersion.value),
  resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
  libraryDependencies ++= {
    val akkaV = akkaVersionFor(scalaVersion.value)
    Seq(
      "com.typesafe.akka" %% "akka-persistence"         % akkaV,
      "com.typesafe.akka" %% "akka-persistence-query"   % akkaV,
      "com.typesafe.akka" %% "akka-persistence-typed"   % akkaV,
      "com.typesafe.akka" %% "akka-stream"              % akkaV,
      "com.typesafe.akka" %% "akka-actor-typed"         % akkaV,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaV % Test,
      "com.typesafe.akka" %% "akka-testkit"             % akkaV % Test
    )
  },
  libraryDependencies += "org.typelevel"     %% "cats-core"                % "2.9.0",
  libraryDependencies += "ch.qos.logback"     % "logback-classic"          % "1.4.7",
  libraryDependencies += "joda-time"          % "joda-time"                % "2.12.5",
  libraryDependencies += "org.reactivemongo" %% "reactivemongo"            % rxmongoVersion,
  libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoStreamVersion,
  libraryDependencies += "org.scalatest"     %% "scalatest"                % "3.2.15" % Test,
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) =>
        Seq(
          "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
          "com.typesafe.play" %% "play-json"    % "2.9.4"
        )
      case Some((3, _)) =>
        Seq(
          "com.typesafe.play" %% "play-json" % "2.10.4"
        )
      case _ => Seq.empty
    }
  },
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  coverageExcludedPackages                   := Seq(
    "<empty>",
    ".*ReactiveMongoJavaReadJournal.*",
    ".*ReactiveMongoDriver.*"
  ).mkString(";"),
  Test / fork                                := true,
  Test / javaOptions += "-Xmx4G",
  Test / javaOptions += "-Dfile.encoding=UTF-8",
  Test / javaOptions += "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
)

lazy val core = (project in file("core"))
  .dependsOn(macros, api)
  .settings(
    commonSettings,
    publishTo                              := Some(
      "nullvector" at (if (isSnapshot.value)
                         "https://nullvectormirror.jfrog.io/artifactory/libs-snapshots"
                       else
                         "https://nullvectormirror.jfrog.io/artifactory/libs-release")
    ),
    credentials += Credentials(Path.userHome / ".jfrog" / "credentials"),
    Compile / packageDoc / publishArtifact := false,
    Compile / packageBin / mappings ++= (macros / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (macros / Compile / packageSrc / mappings).value,
    Compile / packageBin / mappings ++= (api / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (api / Compile / packageSrc / mappings).value
  )

lazy val macros = (project in file("macros"))
  .dependsOn(api)
  .settings(
    commonSettings,
    publish      := {},
    publishLocal := {}
  )

lazy val api = (project in file("api"))
  .settings(
    commonSettings,
    publish      := {},
    publishLocal := {}
  )
