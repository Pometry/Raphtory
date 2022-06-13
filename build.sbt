import sbt.Compile
import sbt.Keys.baseDirectory
import Dependencies._

ThisBuild / scalaVersion := "2.13.7"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.raphtory"
ThisBuild / organizationName := "raphtory"
ThisBuild / organizationHomepage := Some(url("https://raphtory.readthedocs.io/"))

ThisBuild / resolvers += "Mulesoft Repository" at "https://repository.mulesoft.org/nexus/content/repositories/public/"

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

ThisBuild / scmInfo := Some(
        ScmInfo(
                url("https://github.com/Raphtory/Raphtory"),
                "scm:git@github.com:Raphtory/Raphtory.git"
        )
)
ThisBuild / developers := List(
        Developer(
                id = "miratepuffin",
                name = "Ben Steer",
                email = "ben.steer@raphtory.com",
                url = url("https://twitter.com/miratepuffin")
        )
)

ThisBuild / description := "A Distributed Temporal Graph Processing System"
ThisBuild / licenses := List(
        "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/Raphtory/Raphtory"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository.withRank(KeyRanks.Invisible) := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle.withRank(KeyRanks.Invisible) := true

lazy val root = (project in file("."))
  .settings(
          name := "Raphtory",
          defaultSettings
  )
  .aggregate(
          core,
          connectorsAWS,
          connectorsTwitter,
          examplesEnron,
          examplesFacebook,
          examplesGab,
          examplesLotr,
          examplesPresto,
          examplesTwitter,
          deploy
  )

lazy val core = (project in file("core"))
  .settings(
          name := "core",
          assembly / test := {},
          scalafmtOnCompile := true,
          Compile / doc / scalacOptions := Seq(
                  "-skip-packages",
                  "com.raphtory.algorithms.generic:com.raphtory.algorithms.temporal:com.raphtory.algorithms.filters",
                  "-private"
          ),
          assemblySettings,
          defaultSettings,
          libraryDependencies ++= Seq(
                  //please keep in alphabetical order
                  akkaTyped,
                  curatorRecipes,
                  gson,
                  javaxScript,
                  log4jSlft4,
                  log4jApi,
                  log4jCore,
                  openhft,
                  prometheusClient,
                  prometheusHotspot,
                  prometheusHttp,
                  pulsarAdmin,
                  pulsarApi,
                  pulsarCommon,
                  pulsarCrypto,
                  pulsarOriginal,
                  py4j,
                  scalaLogging,
                  scalaTest,
                  scalaTestCompile,
                  slf4j,
                  sprayJson,
                  timeSeries,
                  twitterChill,
                  catsEffect,
                  alleyCats,
                  typesafeConfig,
                  zookeeper
          ),
          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
  )

// CONNECTORS

lazy val connectorsAWS =
  (project in file("connectors/aws")).dependsOn(core).settings(assemblySettings)

lazy val connectorsTwitter =
  (project in file("connectors/twitter")).dependsOn(core).settings(assemblySettings)

// EXAMPLE PROJECTS

lazy val examplesEnron =
  (project in file("examples/raphtory-example-enron")).dependsOn(core).settings(assemblySettings)

lazy val examplesEthereum =
  (project in file("examples/raphtory-example-ethereum")).dependsOn(core).settings(assemblySettings)

lazy val examplesFacebook =
  (project in file("examples/raphtory-example-facebook")).dependsOn(core).settings(assemblySettings)

lazy val examplesGab =
  (project in file("examples/raphtory-example-gab")).dependsOn(core).settings(assemblySettings)

lazy val examplesLotr =
  (project in file("examples/raphtory-example-lotr")).dependsOn(core).settings(assemblySettings)

lazy val examplesPresto =
  (project in file("examples/raphtory-example-presto")).dependsOn(core).settings(assemblySettings)

lazy val examplesTwitter =
  (project in file("examples/raphtory-example-twitter"))
    .dependsOn(core, connectorsTwitter)
    .settings(assemblySettings)

lazy val deploy =
  (project in file("deploy"))
    .settings(assemblySettings)

// SETTINGS

lazy val defaultSettings = Seq(
        scalacOptions := Seq(
                "-feature",
                "-language:implicitConversions",
                "-language:postfixOps",
                "-unchecked",
                "-deprecation",
                "-encoding",
                "utf8"
        )
)

// ASSEMBLY SETTINGS

lazy val assemblySettings = assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")                                                => MergeStrategy.discard
  case x if Assembly.isConfigFile(x)                                                      =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*)                                                      =>
    xs map { _.toLowerCase } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil  =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs                                                      =>
        MergeStrategy.discard
      case "services" :: xs                                                    =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil |
          "io.netty.versions.properties" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _                                                                   => MergeStrategy.first
    }
  case _                                                                                  => MergeStrategy.first
}

Test / parallelExecution := false

Global / concurrentRestrictions := Seq(
        Tags.limit(Tags.Test, 1)
)

// Scaladocs parameters
// doc / scalacOptions ++= Seq("-skip-packages", "com.raphtory.algorithms.generic:com.raphtory.algorithms.temporal", "-private")
