name := "aws"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core"             % "0.2.0-alpha"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3"  % "1.12.221"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-sts" % "1.12.221"
resolvers += Resolver.mavenLocal
