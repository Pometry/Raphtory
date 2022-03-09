name := "example-facebook"
version := "0.5"
organization := "com.raphtory"

assembly / test := {}

Compile / unmanagedJars += baseDirectory.value / "lib/core-assembly-0.5.jar"
Compile / resourceDirectory := baseDirectory.value / "resources"
