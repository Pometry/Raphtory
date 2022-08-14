resolvers += "phdata-sbt-os-detector" at "https://repo.phdata.io/public/sbt-os-detector/maven/"
classpathTypes += "maven-plugin"
addSbtPlugin("io.phdata"        % "sbt-os-detector"    % "0.3.0-20220520.230852-1")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"      % "2.0.0")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"       % "1.1.0")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"       % "2.4.6")
addSbtPlugin("org.xerial.sbt"   % "sbt-sonatype"       % "3.9.5")
addSbtPlugin("com.github.sbt"   % "sbt-pgp"            % "2.1.2")
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
