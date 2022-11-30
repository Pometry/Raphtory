import Version._

name := "it"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion

libraryDependencies ++= Seq(
        "com.raphtory"  %% "core"                       % raphtoryVersion,
        "junit"          % "junit"                      % "4.13.2"  % "test",
        "org.mockito"   %% "mockito-scala"              % "1.17.12" % "test",
        "com.dimafeng"  %% "testcontainers-scala-munit" % "0.40.8"  % "test",
        "org.scalatest" %% "scalatest"                  % "3.2.11"  % "test",
        "org.typelevel" %% "munit-cats-effect-3"        % "1.0.7"   % "test"
)
scalacOptions := Seq(
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-deprecation",
  "-encoding",
  "utf8"
)

resolvers += Resolver.mavenLocal
