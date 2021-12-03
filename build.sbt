import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
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
    assemblyMergeStrategy in assembly := mergeStrategy,
    mainClass in assembly := Some("com.raphtory.core.build.server.RaphtoryGraph"),
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
    libraryDependencies += "com.thesamet.scalapb"          %% "compilerplugin"                    % "0.11.1",
    libraryDependencies += "net.openhft"                   % "zero-allocation-hashing"            % "0.15",
    libraryDependencies += "de.javakaffee"                 % "kryo-serializers"                   % "0.45",
    libraryDependencies += "com.lightbend.akka"           %% "akka-stream-alpakka-avroparquet"    % "3.0.3",
    libraryDependencies += "com.typesafe.akka"            %% "akka-stream"                        % "2.6.14",
    libraryDependencies += "com.twitter"                  %% "chill"                              % "0.10.0",
    libraryDependencies += "com.twitter"                  %% "chill-akka"                         % "0.10.0",
    libraryDependencies += "io.github.kostaskougios"      % "cloning"                             % "1.10.3",
    libraryDependencies += "net.openhft" % "chronicle-map" % "3.22ea5",
    libraryDependencies ++= Seq("com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    Compile / PB.targets := Seq(scalapb.gen() -> (Compile / sourceManaged).value / "scalapb")
  )

// Write custom git hooks to .git/hooks on sbt load
lazy val startupTransition: State => State = { s: State =>
  "writeHooks" :: s
}

onLoad in Global := {
  val old = (onLoad in Global).value
  startupTransition compose old
}
