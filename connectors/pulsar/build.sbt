import Dependencies.excludeLog4j
import Dependencies.excludeSlf4j
import Version._

lazy val pulsarVersion        = "2.9.1"
lazy val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")

name := "pulsar"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion

libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-admin-original"   % pulsarVersion excludeAll excludePulsarBinding
libraryDependencies += "org.apache.pulsar" % "pulsar-client-api"              % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies += "org.apache.pulsar" % "pulsar-common"                  % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-messagecrypto-bc" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-original"         % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies += "com.raphtory"     %% "core"                           % raphtoryVersion
libraryDependencies +=
  "org.typelevel"                         %% "munit-cats-effect-3"            % "1.0.7"
resolvers += Resolver.mavenLocal
