import sbt._

object Dependencies {
  private lazy val akkaVersion                     = "2.6.19"
  private lazy val bcelVersion                     = "6.5.0"
  private lazy val catsEffectVersion               = "3.3.12"
  private lazy val chillVersion                    = "0.10.0"
  private lazy val curatorVersion                  = "5.2.1"
  private lazy val declineVersion                  = "2.3.0"
  private lazy val fs2Version                      = "3.2.12"
  private lazy val jacksonVersion                  = "2.13.3"
  private lazy val jsonpathVersion                  = "0.5.5"
  private lazy val log4jVersion                    = "2.18.0"
  private lazy val muVersion                       = "0.29.0"
  private lazy val openhftVersion                  = "0.15"
  private lazy val pemjaVersion                    = "0.2.0"
  private lazy val prometheusVersion               = "0.15.0"
  private lazy val pulsarVersion                   = "2.9.1"
  private lazy val py4jVersion                     = "0.10.9.5"
  private lazy val scalaLoggingVersion             = "3.9.4"
  private lazy val shapelessVer                    = "2.3.3"
  private lazy val scalaParallelCollectionsVersion = "1.0.4"
  private lazy val scalaPbVersion                  = "0.11.10"
  private lazy val scalatestVersion                = "3.2.11"
  private lazy val slf4jVersion                    = "1.7.36"
  private lazy val sprayJsonVersion                = "1.3.6"
  private lazy val testContainersVersion           = "0.40.8"
  private lazy val timeSeriesVersion               = "1.7.0"
  private lazy val typesafeConfigVersion           = "1.4.2"
  private lazy val ujsonVersion                    = "2.0.0"
  private lazy val zookeeperVersion                = "3.7.0"
  private lazy val catsVersion                     = "2.7.0"
  private lazy val catsMUnitVersion                = "1.0.7"
  private lazy val nomenVersion                    = "2.1.0"

  lazy val excludeSlf4j = ExclusionRule(organization = "org.slf4j")
  lazy val excludeLog4j = ExclusionRule(organization = "log4j")

  lazy val akkaClusterTyped =
    "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val akkaTyped  = "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val bcel       = "org.apache.bcel"    % "bcel"             % bcelVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val catsEffect = "org.typelevel"     %% "cats-effect"      % catsEffectVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val curatorRecipes =
    "org.apache.curator" % "curator-recipes" % curatorVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val decline    = "com.monovore"             %% "decline-effect" % declineVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val fs2        = "co.fs2"                   %% "fs2-core"       % fs2Version
  lazy val apacheHttp = "org.apache.httpcomponents" % "httpclient"     % "4.5.13"

  lazy val jackson =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val jfr = "org.gradle.jfr.polyfill" % "jfr-polyfill" % "1.0.0"
  lazy val jsonpath = "com.jayway.jsonpath" % "json-path" % jsonpathVersion
  lazy val log4jApi   = "org.apache.logging.log4j" % "log4j-api"        % log4jVersion
  lazy val log4jCore  = "org.apache.logging.log4j" % "log4j-core"       % log4jVersion
  lazy val log4jSlft4 = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion

  lazy val magnolia = "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.2" excludeAll (excludeLog4j, excludeSlf4j)

  lazy val muClient  = "io.higherkindness" %% "mu-rpc-client-netty" % muVersion
  lazy val muFs2     = "io.higherkindness" %% "mu-rpc-fs2"          % muVersion
  lazy val muHealth  = "io.higherkindness" %% "mu-rpc-health-check" % muVersion
  lazy val muServer  = "io.higherkindness" %% "mu-rpc-server"       % muVersion
  lazy val muService = "io.higherkindness" %% "mu-rpc-service"      % muVersion

  lazy val nomen = "com.oblac" % "nomen-est-omen" % nomenVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val openhft =
    "net.openhft" % "zero-allocation-hashing" % openhftVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val prometheusClient =
    "io.prometheus" % "simpleclient" % prometheusVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val prometheusHttp =
    "io.prometheus" % "simpleclient_httpserver" % prometheusVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val prometheusHotspot =
    "io.prometheus" % "simpleclient_hotspot" % prometheusVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val pemja = "com.alibaba" % "pemja" % pemjaVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val py4j         = "net.sf.py4j"                 % "py4j"          % py4jVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion

  lazy val scalaParallelCollections =
    "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion
  lazy val scalaPb          = "com.thesamet.scalapb" %% "scalapb-runtime"            % scalaPbVersion        % "protobuf"
  lazy val scalaTest        = "org.scalatest"        %% "scalatest"                  % scalatestVersion      % Test
  lazy val scalaTestCompile = "org.scalatest"        %% "scalatest"                  % scalatestVersion
  lazy val shapeless        = "com.chuusai"          %% "shapeless"                  % shapelessVer
  lazy val slf4j            = "org.slf4j"             % "slf4j-api"                  % slf4jVersion
  lazy val sprayJson        = "io.spray"             %% "spray-json"                 % sprayJsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val testContainers   = "com.dimafeng"         %% "testcontainers-scala-munit" % testContainersVersion % "test"

  lazy val twitterChill   = "com.twitter"         %% "chill"     % chillVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val typesafeConfig = "com.typesafe"         % "config"    % typesafeConfigVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val ujson = "com.lihaoyi"        %% "upickle"  % ujsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val zookeeper      = "org.apache.zookeeper" % "zookeeper" % zookeeperVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val catsMUnit =
    "org.typelevel" %% "munit-cats-effect-3" % catsMUnitVersion % Test excludeAll (excludeLog4j, excludeSlf4j)
  lazy val alleyCats      = "org.typelevel"      %% "alleycats-core"          % catsVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val scalaDocReader = "com.github.takezoe" %% "runtime-scaladoc-reader" % "1.0.3"

  lazy val curatorDiscovery = "org.apache.curator" % "curator-x-discovery" % curatorVersion
}
