import Version._

name := "example-nft"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies += "com.raphtory"       %% "core"                    % raphtoryVersion
resolvers += Resolver.mavenLocal
Compile / resourceDirectory := baseDirectory.value / "resources"
libraryDependencies += "net.sf.py4j"         % "py4j"                    % "0.10.9.5"
libraryDependencies += "com.github.takezoe" %% "runtime-scaladoc-reader" % "1.0.3"
