import sbt.Compile
import sbt.Keys.baseDirectory

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
                  dependencies.curatorRecipes,
                  dependencies.k8Client,
                  dependencies.gson,
                  dependencies.log4jSlft4,
                  dependencies.log4jApi,
                  dependencies.log4jCore,
                  dependencies.monix,
                  dependencies.openhft,
                  dependencies.prometheusClient,
                  dependencies.prometheusHotspot,
                  dependencies.prometheusHttp,
                  dependencies.pulsarAdmin,
                  dependencies.pulsarApi,
                  dependencies.pulsarCommon,
                  dependencies.pulsarCrypto,
                  dependencies.pulsarOriginal,
                  dependencies.py4j,
                  dependencies.scalaLogging,
                  dependencies.scalaTest,
                  dependencies.scalaTestCompile,
                  dependencies.slf4j,
                  dependencies.sprayJson,
                  dependencies.timeSeries,
                  dependencies.twitterChill,
                  dependencies.twittered,
                  dependencies.typesafeConfig,
                  dependencies.zookeeper
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
//                  dependencies.curatorRecipes,
//                  dependencies.k8Client,
//          ),
//          libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
//  )

// EXAMPLE PROJECTS

lazy val examplesEnron    = (project in file("examples/raphtory-example-enron")).dependsOn(core)
lazy val examplesEthereum = (project in file("examples/raphtory-example-ethereum")).dependsOn(core)
lazy val examplesFacebook = (project in file("examples/raphtory-example-facebook")).dependsOn(core)
lazy val examplesGab      = (project in file("examples/raphtory-example-gab")).dependsOn(core)
lazy val examplesLotr     = (project in file("examples/raphtory-example-lotr")).dependsOn(core)
lazy val examplesPresto   = (project in file("examples/raphtory-example-presto")).dependsOn(core)
lazy val examplesTwitter  = (project in file("examples/raphtory-example-twitter")).dependsOn(core)

lazy val examplesTwitterCircles =
  (project in file("examples/raphtory-example-twittercircles")).dependsOn(core)

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

// DEPENDENCIES

lazy val dependencies = new {
  val chillVersion          = "0.10.0"
  val curatorVersion        = "5.2.1"
  val kubernetesClient      = "5.11.1"
  val gsonVersion           = "2.9.0"
  val log4jVersion          = "2.17.1"
  val monixVersion          = "3.4.0"
  val openhftVersion        = "0.15"
  val prometheusVersion     = "0.15.0"
  val pulsarVersion         = "2.9.1"
  val py4jVersion           = "0.10.9.5"
  val scalaLoggingVersion   = "3.9.4"
  val scalatestVersion      = "3.2.11"
  val slf4jVersion          = "1.7.36"
  val sprayJsonVersion      = "1.3.6"
  val timeSeriesVersion     = "1.7.0"
  val twitteredVersion      = "2.16"
  val typesafeConfigVersion = "1.4.2"
  val zookeeperVersion      = "3.7.0"

  val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")
  val excludeSlf4j         = ExclusionRule(organization = "org.slf4j")
  val excludeLog4j         = ExclusionRule(organization = "log4j")

  val curatorRecipes = "org.apache.curator"       % "curator-recipes"   % curatorVersion
  val k8Client       = "io.fabric8"               % "kubernetes-client" % kubernetesClient
  val gson           = "com.google.code.gson"     % "gson"              % gsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  val log4jApi       = "org.apache.logging.log4j" % "log4j-api"         % log4jVersion
  val log4jCore      = "org.apache.logging.log4j" % "log4j-core"        % log4jVersion
  val log4jSlft4     = "org.apache.logging.log4j" % "log4j-slf4j-impl"  % log4jVersion
  val monix          = "io.monix"                %% "monix"             % monixVersion

  val openhft =
    "net.openhft" % "zero-allocation-hashing" % openhftVersion excludeAll (excludeLog4j, excludeSlf4j)

  val prometheusClient  = "io.prometheus" % "simpleclient"            % prometheusVersion
  val prometheusHttp    = "io.prometheus" % "simpleclient_httpserver" % prometheusVersion
  val prometheusHotspot = "io.prometheus" % "simpleclient_hotspot"    % prometheusVersion
  
  val awsSdk            = "com.amazonaws" % "aws-java-sdk-s3" % "1.12.192"


  val pulsarAdmin =
    "org.apache.pulsar" % "pulsar-client-admin-original" % pulsarVersion excludeAll excludePulsarBinding
  val pulsarApi        = "org.apache.pulsar"           % "pulsar-client-api"              % pulsarVersion
  val pulsarCommon     = "org.apache.pulsar"           % "pulsar-common"                  % pulsarVersion
  val pulsarCrypto     = "org.apache.pulsar"           % "pulsar-client-messagecrypto-bc" % pulsarVersion
  val pulsarOriginal   = "org.apache.pulsar"           % "pulsar-client-original"         % pulsarVersion
  val py4j             = "net.sf.py4j"                 % "py4j"                           % py4jVersion excludeAll (excludeLog4j, excludeSlf4j)
  val scalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"                  % scalaLoggingVersion
  val scalaTest        = "org.scalatest"              %% "scalatest"                      % scalatestVersion % Test
  val scalaTestCompile = "org.scalatest"              %% "scalatest"                      % scalatestVersion
  val slf4j            = "org.slf4j"                   % "slf4j-api"                      % slf4jVersion
  val sprayJson        = "io.spray"                   %% "spray-json"                     % sprayJsonVersion

  val timeSeries =
    "io.sqooba.oss" %% "scala-timeseries-lib" % timeSeriesVersion excludeAll (excludeLog4j, excludeSlf4j)
  val twitterChill   = "com.twitter"                 %% "chill"     % chillVersion
  val twittered      = "io.github.redouane59.twitter" % "twittered" % twitteredVersion
  val typesafeConfig = "com.typesafe"                 % "config"    % typesafeConfigVersion
  val zookeeper      = "org.apache.zookeeper"         % "zookeeper" % zookeeperVersion
}

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
