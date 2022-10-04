import Version._

name := "aws"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies += "com.raphtory"      %% "core"              % raphtoryScalaVersion
libraryDependencies += "com.vaticle.typedb" % "typedb-client"     % "2.11.0"
libraryDependencies += "com.univocity"      % "univocity-parsers" % "2.9.1"
libraryDependencies += "org.sharegov"       % "mjson"             % "1.4.1"
resolvers += Resolver.mavenLocal
resolvers += "repo.vaticle.com" at "https://repo.vaticle.com/repository/maven/"
