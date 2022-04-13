name := "example-gab"
version := "0.5"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core" % "0.5"
resolvers += Resolver.mavenLocal
Compile / resourceDirectory := baseDirectory.value / "resources"
