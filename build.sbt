lazy val root = (project in file("."))
  .settings(
      inThisBuild(List(
          organization := "com.raphtory",
          scalaVersion := "2.13.7"
      )),
      name            := "raphtory-pulsar",
      version         := "0.1",
      assembly / test := {}
  )

Test / parallelExecution := false

Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.Test, 1),
)

val pulsarVersion        = "2.9.0"
val pulsar4sVersion      = "2.8.0"
val scalatestVersion     = "3.2.9"
val excludePulsarBinding = ExclusionRule(organization = "org.apache.pulsar")

val excludeSlf4j = ExclusionRule(organization = "org.slf4j")
val excludeLog4j = ExclusionRule(organization = "log4j")


libraryDependencies += "org.apache.pulsar"            %  "pulsar-common"                          % pulsarVersion
libraryDependencies += "com.sksamuel.avro4s" % "avro4s-core_2.13" % "4.0.12"


libraryDependencies += "org.apache.pulsar"            %  "pulsar-io-file"                         % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-functions-api"                   % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-client-api"                      % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-client-original"                 % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-client-messagecrypto-bc"         % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-functions-local-runner-original" % pulsarVersion
libraryDependencies += "org.apache.pulsar"            %  "pulsar-client-admin-original"           % pulsarVersion excludeAll(excludePulsarBinding)
libraryDependencies += "net.openhft"                  %  "zero-allocation-hashing"                % "0.15" excludeAll(excludeLog4j,excludeSlf4j)
libraryDependencies += "org.scalatest"                %% "scalatest"                              % scalatestVersion
libraryDependencies += "org.scalatest"                %% "scalatest"                              % scalatestVersion % Test
libraryDependencies += "org.apache.logging.log4j"     % "log4j-api"                               % "2.17.1"
libraryDependencies += "org.apache.logging.log4j"     % "log4j-core"                              % "2.17.1"
libraryDependencies += "org.apache.logging.log4j"     % "log4j-slf4j-impl"                        % "2.17.1"
libraryDependencies += "org.slf4j"                    % "slf4j-api"                               % "1.7.36"
libraryDependencies += "com.twitter"                  %% "chill"                                  % "0.10.0"
libraryDependencies += "io.spray"                     %% "spray-json"                             % "1.3.6"
libraryDependencies += "org.apache.zookeeper"         % "zookeeper"                               % "3.7.0"
libraryDependencies += "io.github.kostaskougios"      % "cloning"                                 % "1.10.3"
libraryDependencies += "io.fabric8"                   % "kubernetes-client"                       % "5.11.1"
libraryDependencies += "com.typesafe.scala-logging"   %% "scala-logging"                          % "3.9.4"
libraryDependencies += "io.monix"                     %% "monix"                                  % "3.4.0"
libraryDependencies += "com.typesafe"                 % "config"                                   % "1.4.1"


libraryDependencies ~= { _.map(_.exclude("org.slf4j","slf4j-log4j12")) }

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    xs map { _.toLowerCase } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil |
           "io.netty.versions.properties" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}


