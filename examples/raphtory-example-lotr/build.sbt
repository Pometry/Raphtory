name := "example-lotr"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"
resolvers += Resolver.mavenLocal
libraryDependencies += "com.raphtory" %% "core" % "0.1.0"
Compile / resourceDirectory := baseDirectory.value / "resources"
libraryDependencies += "net.sf.py4j"   % "py4j" % "0.10.9.5"
