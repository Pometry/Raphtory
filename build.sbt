import sbt.Compile
import sbt.Keys.baseDirectory
import Dependencies._

lazy val root = (project in file("."))
  .settings(
          name := "Raphtory",
          version := "0.5",
          defaultSettings
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(
          core,
          examplesEnron,
          examplesFacebook,
          examplesGab,
          examplesLotr,
          examplesPresto,
          examplesTwitter,
          examplesTwitterCircles
  )

lazy val core = (project in file("core"))
  .settings(
          name := "core",
          version := "0.5",
          assembly / test := {},
          assemblySettings,
          defaultSettings,
          libraryDependencies ++= Seq(
                  curatorRecipes,
                  k8Client,
                  gson,
                  log4jSlft4,
                  log4jApi,
                  log4jCore,
                  monix,
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
                  twittered,
                  typesafeConfig,
                  zookeeper
          ),
          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
  )

// DEPLOYMENTS

//lazy val core = (project in file("deployment"))
//  .settings(
//          name := "deployment",
//          version := "0.5",
//          assembly / test := {},
//          assemblySettings,
//          defaultSettings,
//          libraryDependencies ++= Seq(
//                  curatorRecipes,
//                  k8Client,
//          ),
//          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
//  )

// EXAMPLE PROJECTS

lazy val examplesEnron           = (project in file("examples/raphtory-example-enron")).dependsOn(core)
lazy val examplesEthereum        = (project in file("examples/raphtory-example-ethereum")).dependsOn(core)
lazy val examplesFacebook        = (project in file("examples/raphtory-example-facebook")).dependsOn(core)
lazy val examplesGab             = (project in file("examples/raphtory-example-gab")).dependsOn(core)
lazy val examplesLotr            = (project in file("examples/raphtory-example-lotr")).dependsOn(core)
lazy val examplesPresto          = (project in file("examples/raphtory-example-presto")).dependsOn(core)
lazy val examplesTwitter         = (project in file("examples/raphtory-example-twitter")).dependsOn(core)
lazy val examplesTwitterCircles  = (project in file("examples/raphtory-example-twittercircles")).dependsOn(core)

// SETTINGS

lazy val defaultSettings = Seq(
        version := "0.5",
        scalaVersion := "2.13.7",
        organization := "com.raphtory",
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
