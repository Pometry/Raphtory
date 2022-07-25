name := "example-nft"
version := "0.5"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core" % "0.1.0"
resolvers += Resolver.mavenLocal
Compile / resourceDirectory := baseDirectory.value / "resources"
libraryDependencies += "net.sf.py4j" % "py4j" % "0.10.9.5"
