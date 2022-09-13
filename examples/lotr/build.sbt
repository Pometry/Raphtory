import Dependencies.catsMUnit

name := "example-lotr"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.8"
resolvers += Resolver.mavenLocal
libraryDependencies += "com.raphtory"  %% "core"           % "0.1.0"
Compile / resourceDirectory := baseDirectory.value / "resources"
libraryDependencies += catsMUnit
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.13.8"
