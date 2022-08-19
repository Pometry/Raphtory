name := "arrow-messaging"
version := "0.1.0"
scalaVersion := "2.13.7"
organization := "com.raphtory"
val catsEffectVersion = "3.3.12"
val catsVersion       = "2.7.0"
val catsMUnitVersion  = "1.0.7"

libraryDependencies ++= Seq(
        "io.netty"                 % "netty-transport-native-unix-common" % "4.1.72.Final"   % "compile" classifier osDetectorClassifier.value,
        "org.apache.arrow"         % "flight-core"                        % "8.0.0" exclude ("io.netty", "netty-transport-native-unix-common"),
        "org.apache.logging.log4j" % "log4j-api"                          % "2.17.2",
        "org.apache.logging.log4j" % "log4j-core"                         % "2.17.2",
        "org.scala-lang"           % "scala-reflect"                      % "2.13.8",
        "com.chuusai"             %% "shapeless"                          % "2.3.3",
        "org.scalatest"           %% "scalatest-funsuite"                 % "3.2.12"         % "test",
        "org.scoverage"           %% "scalac-scoverage-runtime"           % "2.0.0",
        "org.typelevel"           %% "cats-effect"                        % catsEffectVersion,
        "org.typelevel"           %% "alleycats-core"                     % catsVersion,
        "org.typelevel"           %% "munit-cats-effect-3"                % catsMUnitVersion % Test
)

//lazy val root = (project in file("."))
//
//  .configs(IntegrationTest)
//  .settings(
//    Defaults.itSettings,
// )

//coverageEnabled.in(ThisBuild ,Test, test) := true
//coverageMinimumBranchTotal := 80
//coverageMinimumStmtTotal := 80
//coverageHighlighting := true
//coverageFailOnMinimum := false
