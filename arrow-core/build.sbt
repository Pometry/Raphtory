import Version._

name := "arrow-core"
version := raphtoryVersion
scalaVersion := raphtoryScalaVersion
organization := "com.raphtory"

libraryDependencies ++= Seq(
        "org.apache.arrow"   % "arrow-memory-unsafe"     % "7.0.0",
        "org.apache.arrow"   % "arrow-vector"            % "7.0.0",
        "org.apache.arrow"   % "arrow-algorithm"         % "7.0.0",
        "org.apache.arrow"   % "arrow-dataset"           % "7.0.0",
        "net.openhft"        % "chronicle-map"           % "3.21.86",
        "net.openhft"        % "zero-allocation-hashing" % "0.15",
        "it.unimi.dsi"       % "fastutil"                % "8.5.6",
        "org.apache.commons" % "commons-lang3"           % "3.12.0",
        "junit"              % "junit"                   % "4.13.2"
)
