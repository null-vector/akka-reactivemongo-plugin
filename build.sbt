lazy val scala213               = "2.13.11"
lazy val scala300               = "3.3.0"
lazy val supportedScalaVersions = List(scala213)
lazy val akkaVersion            = "2.6.20"
lazy val rxmongoVersion         = "1.1.0-RC11"
lazy val rxmongoStreamVersion   = "1.1.0-RC11"

ThisBuild / scalaVersion       := scala213
ThisBuild / version            := s"1.6.5"
ThisBuild / crossScalaVersions := supportedScalaVersions
ThisBuild / scalacOptions      := Seq(
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

publishArtifact := false
publish         := {}
publishLocal    := {}

lazy val commonSettings = Seq(
  name                                       := "akka-reactivemongo-plugin",
  organization                               := "null-vector",
  resolvers += "Akka Maven Repository" at "https://akka.io/repository",
  libraryDependencies += "com.typesafe.akka" %% "akka-persistence"         % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query"   % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-persistence-typed"   % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream"              % akkaVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
  libraryDependencies += "org.typelevel"     %% "cats-core"                % "2.9.0",
  libraryDependencies += "ch.qos.logback"     % "logback-classic"          % "1.4.7",
  libraryDependencies += "joda-time"          % "joda-time"                % "2.12.5",
  libraryDependencies += "org.reactivemongo" %% "reactivemongo"            % rxmongoVersion,
  libraryDependencies += "org.reactivemongo" %% "reactivemongo-akkastream" % rxmongoStreamVersion,
  libraryDependencies += "io.netty"           % "netty-all"                % "5.0.0.Alpha2",
  libraryDependencies += "com.typesafe.play" %% "play-json"                % "2.9.4",
  libraryDependencies += "org.scala-lang"     % "scala-reflect"            % scalaVersion.value,

  //libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  libraryDependencies += "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  libraryDependencies += "org.scalatest"     %% "scalatest"                % "3.2.15"    % Test,
  libraryDependencies += "com.typesafe.akka" %% "akka-testkit"             % akkaVersion % Test,
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
