import sbt.Compile
import sbt.Keys.baseDirectory
import Dependencies._
import Version._
import higherkindness.mu.rpc.srcgen.Model._
import ReleaseTransformations._
import scala.io.Source

ThisBuild / scalaVersion := raphtoryScalaVersion
ThisBuild / version := raphtoryVersion
ThisBuild / organization := "com.raphtory"
ThisBuild / organizationName := "raphtory"
ThisBuild / organizationHomepage := Some(url("https://raphtory.readthedocs.io/"))
ThisBuild / versionScheme := Some("early-semver")

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

ThisBuild / scmInfo := Some(
        ScmInfo(
                url("https://github.com/Raphtory/Raphtory"),
                "scm:git@github.com:Raphtory/Raphtory.git"
        )
)

ThisBuild / developers := List(
      tlGitHubDev("Pometry-Team", "Pometry"),
      tlGitHubDev("miratepuffin", "Ben Steer")
)

//ThisBuild / tlSonatypeUseLegacyHost := false

ThisBuild / description := "A Distributed Temporal Graph Processing System"
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / homepage := Some(url("https://github.com/Raphtory/Raphtory"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository.withRank(KeyRanks.Invisible) := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
//ThisBuild / resolvers += Resolver.mavenLocal

ThisBuild / scalacOptions += "-language:higherKinds"


ThisBuild / releaseProcess := Seq[ReleaseStep](publishArtifacts)
releasePublishArtifactsAction := PgpKeys.publishSigned.value

//def on[A](major: Int, minor: Int)(a: A): Def.Initialize[Seq[A]] =
//  Def.setting {
//    CrossVersion.partialVersion(scalaVersion.value) match {
//      case Some(v) if v == (major, minor) => Seq(a)
//      case _                              => Nil
//    }
//  }
//
//lazy val macroSettings: Seq[Setting[_]] = Seq(
//        libraryDependencies ++= Seq(
//                scalaOrganization.value % "scala-compiler" % scalaVersion.value % Provided
//        ),
//        libraryDependencies ++= on(2, 12)(
//                compilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full)
//        ).value,
//        scalacOptions ++= on(2, 13)("-Ymacro-annotations").value
//)

lazy val root = (project in file("."))
  .settings(
          name := "Raphtory",
          defaultSettings
  )
  .enablePlugins(OsDetectorPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(
          arrowMessaging,
          arrowCore,
          core,
          connectorsAWS,
          connectorsTwitter,
          examplesCoho,
          connectorsPulsar,
          connectorsTypeDB,
          examplesGab,
          examplesLotr,
          examplesTwitter,
          examplesNFT,
          deploy,
          integrationTest,
          docTests
)

//lazy val protocol = project
//  .settings(
//          // Needed to expand the @service macro annotation
//          macroSettings
//  )

lazy val arrowMessaging =
  (project in file("arrow-messaging"))
    .settings(assemblySettings,
      publishConfiguration := publishConfiguration.value.withOverwrite(true),
      publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
      publishMavenStyle := true
    )

lazy val arrowCore =
  (project in file("arrow-core"))
    .settings(
      assemblySettings,
      publishConfiguration := publishConfiguration.value.withOverwrite(true),
      publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
      publishMavenStyle := true,
)

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
          addCompilerPlugin(scalaDocReader),
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
                  jsonpath,
                  log4jSlft4,
                  log4jApi,
                  log4jCore,
                  magnolia,
                  muClient,
                  muFs2,
                  muHealth,
                  muServer,
                  muService,
                  nomen,
                  openhft,
                  pemja,
                  prometheusClient,
                  prometheusHotspot,
                  prometheusHttp,
                  py4j,
                  scalaLogging,
                  scalaParallelCollections,
                  scalaPb,
                  scalaTest,
                  scalaTestCompile,
                  slf4j,
                  sprayJson,
                  testContainers,
                  twitterChill,
                  ujson,
                  catsEffect,
                  catsMUnit,
                  alleyCats,
                  typesafeConfig,
                  zookeeper,
                  magnolia,
                  shapeless,
                  curatorDiscovery,
                  scalaDocReader,
                  "junit" % "junit" % "4.13.2" % Test
          ),
          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) },
          // Needed to expand the @service macro annotation
//          macroSettings,
          // Generate sources from .proto files
          muSrcGenIdlType := IdlType.Proto,
          // Make it easy for 3rd-party clients to communicate with us via gRPC
          muSrcGenIdiomaticEndpoints := true,
          publishMavenStyle := true,
          publishConfiguration := publishConfiguration.value.withOverwrite(true),
          publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
  )
  .dependsOn(arrowMessaging, arrowCore)
  .enablePlugins(SrcGenPlugin)

// CONNECTORS

lazy val connectorsAWS =
  (project in file("connectors/aws"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core)
    .settings(assemblySettings)

lazy val connectorsTwitter =
  (project in file("connectors/twitter"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core)
    .settings(assemblySettings)

lazy val connectorsTypeDB =
  (project in file("connectors/typedb"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core)
    .settings(assemblySettings)

lazy val connectorsPulsar =
  (project in file("connectors/pulsar"))
    .dependsOn(core % "compile->compile;test->test")
    .enablePlugins(NoPublishPlugin)
    .settings(assemblySettings)

// EXAMPLE PROJECTS

lazy val examplesCoho =
  (project in file("examples/companies-house"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core)
    .settings(assemblySettings)

lazy val examplesGab =
  (project in file("examples/gab"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core, connectorsPulsar)
    .settings(assemblySettings)

lazy val examplesLotr =
  (project in file("examples/lotr"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core % "compile->compile;test->test", connectorsPulsar)
    .settings(assemblySettings)

lazy val examplesTwitter =
  (project in file("examples/twitter"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core, connectorsTwitter, connectorsPulsar)
    .settings(assemblySettings)

lazy val examplesNFT =
  (project in file("examples/nft"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core)
    .settings(assemblySettings)

lazy val deploy =
  (project in file("deploy"))
    .enablePlugins(NoPublishPlugin)
    .settings(assemblySettings)

lazy val integrationTest =
  (project in file("test"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core % "compile->compile;test->test")
    .settings(assemblySettings)

lazy val docTests =
  (project in file("doc-tests"))
    .enablePlugins(NoPublishPlugin)
    .dependsOn(core % "compile->compile;test->test", examplesLotr)
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

Test / parallelExecution := true

Global / concurrentRestrictions := Seq(
        Tags.limit(Tags.Test, 1)
)
core / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
// Scaladocs parameters
doc / scalacOptions ++= Seq("-skip-packages", "com.raphtory.algorithms.generic:com.raphtory.algorithms.temporal", "-private")
ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
