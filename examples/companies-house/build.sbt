import Version._

name := "example-coho"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies += "com.raphtory" %% "core" % raphtoryVersion
resolvers += Resolver.mavenLocal
