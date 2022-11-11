import Version._
import sbt.Keys.libraryDependencies
name := "deploy"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
libraryDependencies += "io.fabric8"                  % "kubernetes-client" % "5.12.2"
libraryDependencies += "com.typesafe"                % "config"            % "1.4.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging"     % "3.9.4"
libraryDependencies += "org.slf4j"                   % "slf4j-api"         % "1.7.36"
libraryDependencies += "org.apache.logging.log4j"    % "log4j-api"         % "2.17.1"
libraryDependencies += "org.apache.logging.log4j"    % "log4j-core"        % "2.17.1"
libraryDependencies += "org.apache.logging.log4j"    % "log4j-slf4j-impl"  % "2.17.1"
resolvers += Resolver.mavenLocal
