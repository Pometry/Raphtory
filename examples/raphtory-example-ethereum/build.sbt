import sbt.Keys.libraryDependencies

name := "example-ethereum"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"
resolvers += Resolver.mavenLocal
libraryDependencies += "com.raphtory" %% "core" % "0.1.0"
Compile / resourceDirectory := baseDirectory.value / "resources"

libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
        "com.typesafe"                % "config"        % "1.4.1"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case _                           => MergeStrategy.first
}
