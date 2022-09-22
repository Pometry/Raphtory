import sbt.Compile
import sbt.Keys.baseDirectory
import Dependencies._
import higherkindness.mu.rpc.srcgen.Model._

import scala.io.Source

val raphtoryVersion = Source.fromFile("version").getLines.next()
ThisBuild / scalaVersion := "2.13.7"
ThisBuild / version := raphtoryVersion
ThisBuild / organization := "com.raphtory"
ThisBuild / organizationName := "raphtory"
ThisBuild / organizationHomepage := Some(url("https://raphtory.readthedocs.io/"))

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

ThisBuild / scalacOptions += "-language:higherKinds"

def on[A](major: Int, minor: Int)(a: A): Def.Initialize[Seq[A]] =
  Def.setting {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some(v) if v == (major, minor) => Seq(a)
      case _                              => Nil
    }
  }

lazy val macroSettings: Seq[Setting[_]] = Seq(
        libraryDependencies ++= Seq(
                scalaOrganization.value % "scala-compiler" % scalaVersion.value % Provided
        ),
        libraryDependencies ++= on(2, 12)(
                compilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full)
        ).value,
        scalacOptions ++= on(2, 13)("-Ymacro-annotations").value
)

lazy val root = (project in file("."))
  .settings(
          name := "Raphtory",
          defaultSettings
  )
  .aggregate(
          core,
          connectorsAWS,
          connectorsTwitter,
          examplesCoho,
          connectorsTypeDB,
          examplesGab,
          examplesLotr,
          examplesTwitter,
          examplesNFT,
          deploy,
          integrationTest
  )

//lazy val protocol = project
//  .settings(
//          // Needed to expand the @service macro annotation
//          macroSettings
//  )

lazy val core = (project in file("core"))
  .settings(
          name := "core",
          assembly / test := {},
          scalafmtOnCompile := false,
          Compile / doc / scalacOptions := Seq(
                  "-skip-packages",
                  "com.raphtory.algorithms.generic:com.raphtory.algorithms.temporal:com.raphtory.algorithms.filters",
                  "-private"
          ),
          assemblySettings,
          defaultSettings,
          libraryDependencies ++= Seq(
                  //please keep in alphabetical order
                  akkaClusterTyped,
                  akkaTyped,
                  bcel,
                  curatorRecipes,
                  decline,
                  fs2,
                  apacheHttp,
                  jackson,
                  jfr,
                  log4jSlft4,
                  log4jApi,
                  log4jCore,
                  magnolia,
                  muClient,
                  muFs2,
                  muServer,
                  muService,
                  nomen,
                  openhft,
                  pemja,
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
                  scalaParallelCollections,
                  scalaTest,
                  scalaTestCompile,
                  slf4j,
                  sprayJson,
                  testContainers,
                  twitterChill,
                  catsEffect,
                  catsMUnit,
                  alleyCats,
                  typesafeConfig,
                  zookeeper
          ),
          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) },
          // Needed to expand the @service macro annotation
          macroSettings,
          // Generate sources from .proto files
          muSrcGenIdlType := IdlType.Proto,
          // Make it easy for 3rd-party clients to communicate with us via gRPC
          muSrcGenIdiomaticEndpoints := true
  )
  .enablePlugins(SrcGenPlugin)

// CONNECTORS

lazy val connectorsAWS =
  (project in file("connectors/aws")).dependsOn(core).settings(assemblySettings)

lazy val connectorsTwitter =
  (project in file("connectors/twitter")).dependsOn(core).settings(assemblySettings)

lazy val connectorsTypeDB =
  (project in file("connectors/typedb")).dependsOn(core).settings(assemblySettings)

// EXAMPLE PROJECTS

lazy val examplesCoho =
  (project in file("examples/companies-house")).dependsOn(core).settings(assemblySettings)

lazy val examplesEthereum =
  (project in file("examples/ethereum")).dependsOn(core).settings(assemblySettings)

lazy val examplesGab =
  (project in file("examples/gab")).dependsOn(core).settings(assemblySettings)

lazy val examplesLotr    =
  (project in file("examples/lotr")).dependsOn(core % "compile->compile;test->test").settings(assemblySettings)

lazy val examplesTwitter =
  (project in file("examples/twitter"))
    .dependsOn(core, connectorsTwitter)
    .settings(assemblySettings)

lazy val examplesNFT =
  (project in file("examples/raphtory-example-nft"))
    .dependsOn(core)
    .settings(assemblySettings)

lazy val deploy =
  (project in file("deploy"))
    .settings(assemblySettings)

lazy val integrationTest =
  (project in file("test"))
    .dependsOn(core % "compile->compile;test->test")
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
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil                         =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa")                        =>
        MergeStrategy.discard
      case "plexus" :: xs                                                                             =>
        MergeStrategy.discard
      case "services" :: xs                                                                           =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil | "io.netty.versions.properties" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _                                                                                          => MergeStrategy.first
    }
  case _                                                                                  => MergeStrategy.first
}

Test / parallelExecution := false

Global / concurrentRestrictions := Seq(
        Tags.limit(Tags.Test, 1)
)
core / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
// Scaladocs parameters
// doc / scalacOptions ++= Seq("-skip-packages", "com.raphtory.algorithms.generic:com.raphtory.algorithms.temporal", "-private")
