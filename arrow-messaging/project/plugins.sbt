resolvers += "phdata-sbt-os-detector" at "https://repo.phdata.io/public/sbt-os-detector/maven/"
classpathTypes += "maven-plugin"
addSbtPlugin("io.phdata" % "sbt-os-detector" % "0.3.0-20220520.230852-1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.0")
