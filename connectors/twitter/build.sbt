import Version._

name := "twitter"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies += "com.raphtory"                %% "core"      % raphtoryVersion
libraryDependencies += "io.github.redouane59.twitter" % "twittered" % "2.16"
resolvers += Resolver.mavenLocal
