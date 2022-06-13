import sbt._

object Dependencies {
  private lazy val akkaTypedVersion      = "2.6.19"
  private lazy val catsEffectVersion     = "3.3.12"
  private lazy val chillVersion          = "0.10.0"
  private lazy val curatorVersion        = "5.2.1"
  private lazy val javaxScriptVersion    = "1.1"
  private lazy val gsonVersion           = "2.9.0"
  private lazy val log4jVersion          = "2.17.2"
  private lazy val monixVersion          = "3.4.0"
  private lazy val openhftVersion        = "0.15"
  private lazy val prometheusVersion     = "0.15.0"
  private lazy val pulsarVersion         = "2.9.1"
  private lazy val py4jVersion           = "0.10.9.5"
  private lazy val scalaLoggingVersion   = "3.9.4"
  private lazy val scalatestVersion      = "3.2.11"
  private lazy val slf4jVersion          = "1.7.36"
  private lazy val sprayJsonVersion      = "1.3.6"
  private lazy val timeSeriesVersion     = "1.7.0"
  private lazy val typesafeConfigVersion = "1.4.2"
  private lazy val zookeeperVersion      = "3.7.0"
  private lazy val catsVersion           = "2.7.0"


  lazy val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")
  lazy val excludeSlf4j         = ExclusionRule(organization = "org.slf4j")
  lazy val excludeLog4j         = ExclusionRule(organization = "log4j")

  lazy val akkaTyped      = "com.typesafe.akka" %% "akka-actor-typed" % akkaTypedVersion
  lazy val catsEffect     = "org.typelevel"     %% "cats-effect"      % catsEffectVersion
  lazy val curatorRecipes = "org.apache.curator" % "curator-recipes"  % curatorVersion

  lazy val javaxScript =
    "javax.script" % "js-engine" % javaxScriptVersion //Creates a fake POM to avoid the logger throwing a class not found exception

  lazy val gson =
    "com.google.code.gson" % "gson" % gsonVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val log4jApi   = "org.apache.logging.log4j" % "log4j-api"        % log4jVersion
  lazy val log4jCore  = "org.apache.logging.log4j" % "log4j-core"       % log4jVersion
  lazy val log4jSlft4 = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion

  lazy val openhft =
    "net.openhft" % "zero-allocation-hashing" % openhftVersion excludeAll (excludeLog4j, excludeSlf4j)

  lazy val prometheusClient  = "io.prometheus" % "simpleclient"            % prometheusVersion
  lazy val prometheusHttp    = "io.prometheus" % "simpleclient_httpserver" % prometheusVersion
  lazy val prometheusHotspot = "io.prometheus" % "simpleclient_hotspot"    % prometheusVersion

  lazy val pulsarAdmin =
    "org.apache.pulsar" % "pulsar-client-admin-original" % pulsarVersion excludeAll excludePulsarBinding
  lazy val pulsarApi        = "org.apache.pulsar"           % "pulsar-client-api"              % pulsarVersion
  lazy val pulsarCommon     = "org.apache.pulsar"           % "pulsar-common"                  % pulsarVersion
  lazy val pulsarCrypto     = "org.apache.pulsar"           % "pulsar-client-messagecrypto-bc" % pulsarVersion
  lazy val pulsarOriginal   = "org.apache.pulsar"           % "pulsar-client-original"         % pulsarVersion
  lazy val py4j             = "net.sf.py4j"                 % "py4j"                           % py4jVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val scalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"                  % scalaLoggingVersion
  lazy val scalaTest        = "org.scalatest"              %% "scalatest"                      % scalatestVersion % Test
  lazy val scalaTestCompile = "org.scalatest"              %% "scalatest"                      % scalatestVersion
  lazy val slf4j            = "org.slf4j"                   % "slf4j-api"                      % slf4jVersion
  lazy val sprayJson        = "io.spray"                   %% "spray-json"                     % sprayJsonVersion

  lazy val timeSeries =
    "io.sqooba.oss" %% "scala-timeseries-lib" % timeSeriesVersion excludeAll (excludeLog4j, excludeSlf4j)
  lazy val twitterChill   = "com.twitter"         %% "chill"     % chillVersion
  lazy val typesafeConfig = "com.typesafe"         % "config"    % typesafeConfigVersion
  lazy val zookeeper      = "org.apache.zookeeper" % "zookeeper" % zookeeperVersion
  lazy val alleyCats = "org.typelevel" %% "alleycats-core" % catsVersion

}
