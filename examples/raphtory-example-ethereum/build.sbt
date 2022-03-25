import sbt.Keys.libraryDependencies

name := "example-ethereum"
version := "0.5"
organization := "com.raphtory"

scalaVersion := "2.13.7"

assembly / test := {}

Compile / unmanagedJars += baseDirectory.value / "lib/core-assembly-0.5.jar"
Compile / resourceDirectory := baseDirectory.value / "resources"

libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
        "com.typesafe"                % "config"        % "1.4.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case _                           => MergeStrategy.first
}
