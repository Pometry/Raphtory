import sbt._

object Dependencies {
  private lazy val bcelVersion                     = "6.5.0"
  private lazy val catsEffectVersion               = "3.3.12"
  private lazy val chillVersion                    = "0.10.0"
  private lazy val curatorVersion                  = "5.4.0"
  private lazy val declineVersion                  = "2.3.0"
  private lazy val fs2Version                      = "3.2.12"
  private lazy val h2Version                       = "2.1.214"
  private lazy val jacksonVersion                  = "2.13.3"
  private lazy val jsonpathVersion                 = "0.5.5"
  private lazy val log4jVersion                    = "2.18.0"
  private lazy val muVersion                       = "0.29.1"
  private lazy val openhftVersion                  = "0.15"
  private lazy val pemjaVersion                    = "0.2.6"
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
  private lazy val mockitoScalaVersion             = "1.17.12"
  private lazy val junitVersion                    = "4.13.2"
  private lazy val commonsIOVersion                = "2.11.0"
  private lazy val amazonAws                       = "1.12.221"
  private lazy val twitterVersion                  = "2.16"
  private lazy val typedbClientVersion             = "2.14.2"
  private lazy val univocityParsersVersion         = "2.9.1"
  private lazy val mjsonVersion                    = "1.4.1"
  private lazy val scalaReflectVersion             = "2.13.8"
  private lazy val scalaTestFunSuiteVersion        = "3.2.12"
  private lazy val objenesisVersion                = "3.3"
  private lazy val flightCoreVersion               = "8.0.0"
  private lazy val nettyVersion                    = "4.1.72.Final"
  private lazy val arrowVersion                    = "9.0.0"
  private lazy val chronicleMapVersion             = "3.21.86"
  private lazy val fastUtilVersion                 = "8.5.6"
  private lazy val commonsLangVersion              = "3.12.0"
  private lazy val junitInterfaceVersion           = "0.11"

  lazy val excludeSlf4j         = ExclusionRule(organization = "org.slf4j")
  lazy val excludeLog4j         = ExclusionRule(organization = "log4j")
  lazy val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")
  lazy val excludeJunit         = ExclusionRule(organization = "junit")
  lazy val excludeJunitDep      = ExclusionRule(organization = "junit-dep")

  lazy val bcel       = "org.apache.bcel" % "bcel"        % bcelVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val catsEffect = "org.typelevel"  %% "cats-effect" % catsEffectVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val curatorRecipes =
    "org.apache.curator" % "curator-recipes" % curatorVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val decline    = "com.monovore"             %% "decline-effect" % declineVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val fs2        = "co.fs2"                   %% "fs2-core"       % fs2Version
  lazy val h2         = "com.h2database"            % "h2"             % h2Version % Test
  lazy val apacheHttp = "org.apache.httpcomponents" % "httpclient"     % "4.5.13"

  lazy val jackson =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val jfr        = "org.gradle.jfr.polyfill"  % "jfr-polyfill"     % "1.0.0"
  lazy val jsonpath   = "com.jayway.jsonpath"      % "json-path"        % jsonpathVersion
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
  lazy val scalaTest        = "org.scalatest"        %% "scalatest"                  % scalatestVersion      % "it,test"
  lazy val scalaTestCompile = "org.scalatest"        %% "scalatest"                  % scalatestVersion      % "it,test"
  lazy val shapeless        = "com.chuusai"          %% "shapeless"                  % shapelessVer
  lazy val slf4j            = "org.slf4j"             % "slf4j-api"                  % slf4jVersion
  lazy val sprayJson        = "io.spray"             %% "spray-json"                 % sprayJsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val testContainers   = "com.dimafeng"         %% "testcontainers-scala-munit" % testContainersVersion % "it,test"

  lazy val twitterChill   = "com.twitter"         %% "chill"     % chillVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val typesafeConfig = "com.typesafe"         % "config"    % typesafeConfigVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val ujson          = "com.lihaoyi"         %% "upickle"   % ujsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val zookeeper      = "org.apache.zookeeper" % "zookeeper" % zookeeperVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val catsMUnit =
    "org.typelevel" %% "munit-cats-effect-3" % catsMUnitVersion % "it,test" excludeAll (excludeLog4j, excludeSlf4j)
  lazy val alleyCats      = "org.typelevel"      %% "alleycats-core"          % catsVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val scalaDocReader = "com.github.takezoe" %% "runtime-scaladoc-reader" % "1.0.3"

  lazy val curatorDiscovery = "org.apache.curator" % "curator-x-discovery" % curatorVersion
  lazy val junit            = "junit"              % "junit"               % junitVersion        % "it,test"
  lazy val mockitoScala     = "org.mockito"       %% "mockito-scala"       % mockitoScalaVersion % "it,test"

  // AWS CONNECTOR
  lazy val commonsIO    = "commons-io"    % "commons-io"       % commonsIOVersion
  lazy val amazonAwsS3  = "com.amazonaws" % "aws-java-sdk-s3"  % amazonAws
  lazy val amazonAwsSts = "com.amazonaws" % "aws-java-sdk-sts" % amazonAws

  // PULSAR CONNECTOR
  lazy val pulsarClientAdmin =
    "org.apache.pulsar" % "pulsar-client-admin-original" % pulsarVersion excludeAll excludePulsarBinding

  lazy val pulsarClientApi =
    "org.apache.pulsar" % "pulsar-client-api" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val pulsarCommon = "org.apache.pulsar" % "pulsar-common" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val pulsarClientMsgCrypto =
    "org.apache.pulsar" % "pulsar-client-messagecrypto-bc" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val pulsarClientOriginal =
    "org.apache.pulsar" % "pulsar-client-original" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)

  // TWITTER CONNECTOR
  lazy val twitterEd = "io.github.redouane59.twitter" % "twittered" % twitterVersion

  // TYPEDB CONNECTOR
  lazy val typedbClient     = "com.vaticle.typedb" % "typedb-client"     % typedbClientVersion
  lazy val univocityParsers = "com.univocity"      % "univocity-parsers" % univocityParsersVersion
  lazy val mjson            = "org.sharegov"       % "mjson"             % mjsonVersion

  // ARROW MESSAGING
  lazy val objenesis         = "org.objenesis"  % "objenesis"          % objenesisVersion
  lazy val scalaTestFunSuite = "org.scalatest" %% "scalatest-funsuite" % scalaTestFunSuiteVersion % "test"
  lazy val scalaReflect      = "org.scala-lang" % "scala-reflect"      % scalaReflectVersion

  lazy val netty =
    "io.netty" % "netty-transport-native-unix-common" % nettyVersion % "compile"

  lazy val flightCore =
    "org.apache.arrow" % "flight-core" % flightCoreVersion exclude ("io.netty", "netty-transport-native-unix-common")

  // ARROW CORE
  lazy val arrowMemory    = "org.apache.arrow"   % "arrow-memory-unsafe" % arrowVersion
  lazy val arrowVector    = "org.apache.arrow"   % "arrow-vector"        % arrowVersion
  lazy val arrowAlgorithm = "org.apache.arrow"   % "arrow-algorithm"     % arrowVersion
  lazy val arrowDataset   = "org.apache.arrow"   % "arrow-dataset"       % arrowVersion
  lazy val chronicleMap   = "net.openhft"        % "chronicle-map"       % chronicleMapVersion
  lazy val fastUtil       = "it.unimi.dsi"       % "fastutil"            % fastUtilVersion
  lazy val commonsLang    = "org.apache.commons" % "commons-lang3"       % commonsLangVersion

  lazy val junitInterface =
    "com.novocode" % "junit-interface" % junitInterfaceVersion % Test excludeAll (excludeJunit, excludeJunitDep)

  // Dependencies whose scope goes beyond tests in some modules can go here because
  // it doesn't make any sense to widen the scope of test dependencies to be made part of raphtory
  object Testkit {
    lazy val catsMUnit = "org.typelevel" %% "munit-cats-effect-3" % catsMUnitVersion
  }
}
