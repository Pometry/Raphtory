import Dependencies.excludeLog4j
import Dependencies.excludeSlf4j

lazy val pulsarVersion        = "2.9.1"
lazy val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")

name := "pulsar"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"

libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-admin-original"   % pulsarVersion excludeAll excludePulsarBinding
libraryDependencies += "org.apache.pulsar" % "pulsar-client-api"              % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies += "org.apache.pulsar" % "pulsar-common"                  % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-messagecrypto-bc" % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies +=
  "org.apache.pulsar"                      % "pulsar-client-original"         % pulsarVersion excludeAll (excludeLog4j, excludeSlf4j)
libraryDependencies += "com.raphtory"     %% "core"                           % "0.1.0"
libraryDependencies +=
  "org.typelevel"                         %% "munit-cats-effect-3"            % "1.0.7"
resolvers += Resolver.mavenLocal
