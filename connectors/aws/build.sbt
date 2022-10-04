import Version._

name := "aws"
version := raphtoryVersion
organization := "com.raphtory"
scalaVersion := raphtoryScalaVersion
libraryDependencies += "com.raphtory" %% "core"             % raphtoryVersion
libraryDependencies += "commons-io"    % "commons-io"       % "2.11.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3"  % "1.12.221"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-sts" % "1.12.221"
resolvers += Resolver.mavenLocal
