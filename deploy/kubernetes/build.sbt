name := "deploy"
version := "0.5"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "io.fabric8"         % "kubernetes-client" % "5.12.2"
libraryDependencies += "com.typesafe"         % "config"           % "1.4.2"
libraryDependencies +=  "com.typesafe.scala-logging" %% "scala-logging"                  % "3.9.4"
resolvers += Resolver.mavenLocal
