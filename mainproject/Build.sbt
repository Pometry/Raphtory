	import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}


	val Akka            = "2.5.23"
	val Config          = "1.2.1"
	val JodaT           = "2.3"
	val Logback         = "1.1.2"
	val Scala           = "2.12.4"
	val Slf4j           = "1.7.7"
	//val ScalaJack		    = "4.0"

	val SbtPackager 	  = "1.2.0"


	val resolutionRepos = Seq(
		"Typesafe Repo" 	at "http://repo.typesafe.com/typesafe/releases/",
		"Akka Snapshots"	at "http://repo.akka.io/snapshots/",
		"OSS"						  at "http://oss.sonatype.org/content/repositories/releases",
		"Mvn" 						at "http://mvnrepository.com/artifact"  // for commons_exec
	)

	def dep_compile   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")
	def dep_test      (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")  // test vs test so as not to confuse w/sbt 'test'

	val akka_actor		  = "com.typesafe.akka"		%% "akka-actor"		      % Akka
	val akka_slf4j 		  = "com.typesafe.akka" 	%% "akka-slf4j"		      % Akka
	val akka_remote		  = "com.typesafe.akka" 	%% "akka-remote"	      % Akka
	val akka_cluster	  = "com.typesafe.akka" 	%% "akka-cluster" 	    % Akka
	val akka_tools 		  = "com.typesafe.akka"		%% "akka-cluster-tools" % Akka
	val akka_dist_data  = "com.typesafe.akka" 	%% "akka-distributed-data" % Akka
	val akka_actor_typed= "com.typesafe.akka"   %% "akka-actor-typed"   % Akka
	val akka_cluster_typed = "com.typesafe.akka" %% "akka-cluster-typed" % Akka
	val akka_http       =  "com.typesafe.akka" %% "akka-http" % "10.1.11"
	val akka_management = "com.lightbend.akka.management" %% "akka-management" % "1.0.5"
	val akka_management2= "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % "1.0.5"
	val kube = "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % "1.0.5"



	val typesafe_config	= "com.typesafe"			  %  "config"			        % Config


	val spray_json      = "io.spray"            % "spray-json_2.12"     % "1.3.3"
	//val scalajack		    = "co.blocke"           % "scalajack_2.12"      % "4.1"
	val logback			    = "ch.qos.logback" 			% "logback-classic"	    % Logback
	val slf4j_simple 	  = "org.slf4j" 					% "slf4j-api" 					% "1.7.25"
	val apacheLang 			= "commons-lang" 				%  "commons-lang" 			% "2.6"
	val joda 					  = "joda-time"           % "joda-time"           % "2.10.5"

	val kafka						= "org.apache.kafka" 	 %% "kafka" 							% "2.3.0"
	val kafkac 			  	= "org.apache.kafka"    % "kafka-clients" 			% "2.3.0"

	val kamon						= "io.kamon"					 %% "kamon-core"					% "1.1.0"
	val kamon_prometheus= "io.kamon"					 %% "kamon-prometheus"		% "1.0.0"
  val kamon_akka      = "io.kamon"					 %% "kamon-akka-2.5"		  % "1.0.1"
	val kamon_system 		= "io.kamon" 					 %% "kamon-system-metrics"% "1.0.0"
  val monix						= "io.monix" 					 %% "monix" 						  % "3.0.0-RC1"
	val mongo 					= "org.mongodb" % "mongo-java-driver" % "3.8.0"
	val casbah 					= "org.mongodb" %% "casbah-core" % "3.1.1"

	val doobie 					= "org.tpolecat" 			 %% "doobie-core"      		% "0.8.4"
  val doobiepostgres  = "org.tpolecat" 			 %% "doobie-postgres"  		% "0.8.4"          // Postgres driver 42.2.8 + type mappings.
	val lift 						= "net.liftweb" %% "lift-json" % "3.3.0"

	val bitcoin 			  = "org.scalaj" %% "scalaj-http" % "2.3.0"
	val twitter_eval 		= "com.twitter" %% "util-eval" % "6.43.0"

	val IP = java.net.InetAddress.getLocalHost().getHostAddress()

	lazy val basicSettings = Seq(
 		organization 				:= "com.raphtory",
		description 				:= "Raphtory Distributed Graph Stream Processing",
		startYear 					:= Some(2014),
		scalaVersion 				:= Scala,
		packageName         := "raphtory",
		parallelExecution in Test 	:= false,
		//resolvers					++= Dependencies.resolutionRepos,
		//resolvers					  ++= kamon_repos,
		scalacOptions				:= Seq("-feature", "-deprecation", "-encoding", "UTF8", "-unchecked"),
		testOptions in Test += Tests.Argument("-oDF"),
		version 					:= "ben"

	)

	lazy val dockerStuff = Seq(
		maintainer := "Ben Steer <b.a.steer@qmul.ac.uk>",
		dockerBaseImage := "miratepuffin/raphtory-redis:latest",
        dockerRepository := Some("miratepuffin"),
		dockerExposedPorts := Seq(2551,8080,2552, 1600, 11600)

	)

	val newAnalyser = "com/raphtory/core/actors/analysismanager/TestAnalyser2.scala"

	lazy val root = Project(id = "raphtory",
		base = file(".")) aggregate(cluster)

	lazy val cluster = project.in(file("cluster"))
		.enablePlugins(JavaAppPackaging)
		.enablePlugins(AshScriptPlugin)
	  	.enablePlugins(JavaAgent)
		.settings(isSnapshot := true)
		.settings(dockerStuff:_*)
  	.settings(mappings in Universal +=
			file(s"${baseDirectory.value}/../Build-Scripts/env-setter.sh") -> "bin/env-setter.sh")
		.settings(mappings in Universal +=
			file(s"${baseDirectory.value}/src/main/scala/$newAnalyser") -> newAnalyser)
		.settings(dockerEntrypoint := Seq("bash"))
		.settings(dockerCommands ++= Seq(
			Cmd("ENV", "PATH=/opt/docker/bin:${PATH}"),
			Cmd("RUN", "chmod 755 bin/env-setter.sh")
		))
		.settings(basicSettings: _*)
		.settings(libraryDependencies ++=
			dep_compile(
				typesafe_config, akka_actor, akka_cluster, akka_tools, akka_dist_data, akka_http, akka_remote, akka_slf4j, logback,spray_json,akka_management,akka_management2,
				kube,kamon, kamon_akka, kamon_prometheus, kamon_system, monix,bitcoin,twitter_eval,lift, apacheLang,kafka,kafkac,doobie,doobiepostgres,joda,
				casbah,mongo)
		)
	  	.settings(
          javaAgents += "org.aspectj" % "aspectjweaver" % "1.8.13",
          javaOptions in Universal += "-Dorg.aspectj.tracing.factory=default"
			)
		//.settings((Keys.test in Test) <<= (Keys.test in Test) dependsOn (publishLocal in Docker))
