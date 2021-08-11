import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
import com.typesafe.sbt.packager.docker.Cmd
import sbtassembly.MergeStrategy

lazy val root = Project(id = "raphtory", base = file(".")) aggregate (raphtory)

val resolutionRepos = Seq(
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Snapshots" at "http://repo.akka.io/snapshots/",
  "OSS" at "http://oss.sonatype.org/content/repositories/releases",
  "Mvn" at "http://mvnrepository.com/artifact" // for commons_exec
)

lazy val globalSettings = Seq(
  organization := "com.raphtory",
  startYear := Some(2014),
  scalaVersion := "2.12.4",
  packageName := "raphtory",
  parallelExecution in Test := false,
  scalacOptions := Seq(
    "-feature",
    "-deprecation",
    "-encoding",
    "UTF8",
    "-unchecked"
  ),
  testOptions in Test += Tests.Argument("-oDF"),
  version := "dev",
  // docker settings
  maintainer := "Ben Steer <ben.steer@pometry.com>",
  dockerBaseImage := "miratepuffin/raphtory-redis:latest",
  dockerExposedPorts := Seq(2551, 8080, 2552,25520, 1600, 11600,8081,46339,9100),
  dockerRepository := Some("miratepuffin"),
  dockerEntrypoint := Seq("bash"),
  dockerCommands ++= Seq(
    Cmd("ENV", "PATH=/opt/docker/bin:${PATH}"),
    Cmd("RUN", "chmod 755 bin/env-setter.sh")
  )
)

lazy val mergeStrategy: String => MergeStrategy = {
  case PathList(xs @ _*) if xs.last == "io.netty.versions.properties" => MergeStrategy.first
  case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
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

lazy val raphtory = project
  .in(
    file(".")
  )
  .enablePlugins(
    JavaAppPackaging,
    AshScriptPlugin,
    JavaAgent
  )
  .settings(globalSettings:_*)
  .settings(
    name        := "Raphtory",
    description := "Raphtory Distributed Graph Stream Processing",
    isSnapshot := true,
    mappings in Universal += file(s"${baseDirectory.value}/Build-Scripts/env-setter.sh") -> "bin/env-setter.sh",
    assemblyMergeStrategy in assembly := mergeStrategy,
    mainClass in assembly := Some("com.raphtory.Go"),
    javaOptions in Universal += "-Dorg.aspectj.tracing.factory=default",
    javaAgents +=          "org.aspectj"                   % "aspectjweaver"                      % "1.8.13",
    libraryDependencies += "org.scala-lang"                % "scala-reflect"                      % "2.12.4",
    libraryDependencies += "org.scala-lang"                % "scala-compiler"                     % "2.12.4",
    libraryDependencies += "com.typesafe"                  % "config"                             % "1.2.1",
    libraryDependencies += "com.typesafe.akka"             %% "akka-actor"                        % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-slf4j"                        % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-remote"                       % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-cluster"                      % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-cluster-tools"                % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-distributed-data"             % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-actor-typed"                  % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-cluster-typed"                % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-discovery"                    % "2.6.14",
    libraryDependencies += "com.typesafe.akka"             %% "akka-http"                         % "10.2.4",
    libraryDependencies += "com.typesafe.akka"             %% "akka-http-spray-json"              % "10.2.4",
    libraryDependencies += "ch.qos.logback"                % "logback-classic"                    % "1.2.3",
    libraryDependencies += "io.spray"                      % "spray-json_2.12"                    % "1.3.6",
    libraryDependencies += "com.lightbend.akka.management" %% "akka-management"                   % "1.1.0",
    libraryDependencies += "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % "1.1.0",
    libraryDependencies += "com.lightbend.akka.discovery"  %% "akka-discovery-kubernetes-api"     % "1.1.0",
    libraryDependencies += "io.kamon"                      %% "kamon-core"                        % "2.1.0",
    libraryDependencies += "io.kamon"                      %% "kamon-prometheus"                  % "2.1.0",
    libraryDependencies += "io.kamon"                      %% "kamon-akka"                        % "2.1.0",
    libraryDependencies += "io.kamon"                      %% "kamon-system-metrics"              % "2.1.0",
    libraryDependencies += "net.liftweb"                   %% "lift-json"                         % "3.3.0",
    libraryDependencies += "commons-lang"                  % "commons-lang"                       % "2.6",
    libraryDependencies += "org.apache.kafka"              %% "kafka"                             % "2.5.0",
    libraryDependencies += "org.apache.kafka"              % "kafka-clients"                      % "2.5.0",
    libraryDependencies += "joda-time"                     % "joda-time"                          % "2.10.5",
    libraryDependencies += "org.mongodb"                   %% "casbah-core"                       % "3.1.1",
    libraryDependencies += "org.mongodb"                   % "mongo-java-driver"                  % "3.12.4",
    libraryDependencies += "org.mongodb.scala"             %% "mongo-scala-driver"                % "2.9.0",
    libraryDependencies += "com.github.mjakubowski84"      %% "parquet4s-core"                    % "1.6.0",
    libraryDependencies += "org.apache.hadoop"             % "hadoop-client"                      % "3.3.0",
    libraryDependencies += "io.altoo"                      %% "akka-kryo-serialization"           % "2.2.0",
    libraryDependencies += "com.thesamet.scalapb"          %% "compilerplugin"                    % "0.11.1",
    libraryDependencies += "net.openhft"                   % "zero-allocation-hashing"            % "0.15",
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    ),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
  )
