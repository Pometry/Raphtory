name := "example-presto"
version := "0.5"
organization := "com.raphtory"

assembly / test := {}
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core" % "0.5"
Compile / resourceDirectory := baseDirectory.value / "resources"
