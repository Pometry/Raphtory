import Version._

name := "example-lotr"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
resolvers += Resolver.mavenLocal
libraryDependencies += "com.raphtory" %% "core" % raphtoryVersion
Compile / resourceDirectory := baseDirectory.value / "resources"
