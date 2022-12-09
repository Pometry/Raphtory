// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.raphtory"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("raphtory", "raphtory", "admin@pometry.com"))

developers := List(
  Developer(
    id = "miratepuffin",
    name = "Ben Steer",
    email = "ben.steer@raphtory.com",
    url = url("https://twitter.com/miratepuffin")
  )
)
