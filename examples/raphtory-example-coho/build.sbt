name := "example-coho"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core"    % "0.1.0"
resolvers += Resolver.mavenLocal
Compile / resourceDirectory := baseDirectory.value / "resources"