import sbt._
import Keys._    
import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.packager.archetypes._
import com.typesafe.sbt.packager.docker._

object Versions {
	val Akka            = "2.5.2"
	val Config          = "1.2.1"
	val JodaT           = "2.3"
	val Logback         = "1.1.2"
	val Scala           = "2.11.8"
	val ScalaTest       = "2.2.4"
	val Slf4j           = "1.7.7"
	val ScalaJack		= "4.0"
	val SbtPackager 	= "1.2.0"
}


object Dependencies {
	import Versions._

	val resolutionRepos = Seq(
		"Typesafe Repo" 	at "http://repo.typesafe.com/typesafe/releases/",
		"Akka Snapshots"	at "http://repo.akka.io/snapshots/",
		"OSS"				at "http://oss.sonatype.org/content/repositories/releases",
		"Mvn" 				at "http://mvnrepository.com/artifact"  // for commons_exec
	)

	def dep_compile   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")
	def dep_test      (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")  // ttest vs test so as not to confuse w/sbt 'test'

	val akka_actor		  = "com.typesafe.akka"		%% "akka-actor"		      % Akka
	val akka_slf4j 		  = "com.typesafe.akka" 	%% "akka-slf4j"		      % Akka
	val akka_remote		  = "com.typesafe.akka" 	%% "akka-remote"	      % Akka
	val akka_cluster	  = "com.typesafe.akka" 	%% "akka-cluster" 	    % Akka
	val akka_contrib	  = "com.typesafe.akka" 	%% "akka-contrib" 	    % Akka
	val akka_tools 		  = "com.typesafe.akka"		%% "akka-cluster-tools" % Akka
  val akka_streams    = "com.typesafe.akka"   %  "akka-stream_2.11"   % "2.5.2"
  val akka_http       = "com.typesafe.akka"   % "akka-http_2.11"      % "10.0.7"
	val typesafe_config	= "com.typesafe"			  %  "config"			        % Config
	val spray_json      = "io.spray"            % "spray-json_2.11"     % "1.3.3"
  //val  kafka          = "org.apache.kafka"    % "kafka_2.10" % "0.10.2.1"
	val scalajack		    = "co.blocke"			    	%% "scalajack"		      % ScalaJack
	val logback			    = "ch.qos.logback" 			% "logback-classic"	    % Logback

	val scalatest 		  = "org.scalatest" 			%% "scalatest"		      % ScalaTest
	val slf4j_simple 	  = "org.slf4j" 				  % "slf4j-simple" 	      % Slf4j

	//Zookeeper tings
	val curator1        = "org.apache.curator"  % "curator-framework"   % "2.12.0"
	val curator2        = "org.apache.curator"  % "curator-recipes"     % "2.12.0"
}


object Build extends Build {
	import Dependencies._
	import Versions._

	val IP = java.net.InetAddress.getLocalHost().getHostAddress()

	lazy val basicSettings = Seq(
 		organization 				:= "com.gwz",
		description 				:= "Playing with Docker",
		startYear 					:= Some(2014),
		scalaVersion 				:= Scala,
		parallelExecution in Test 	:= false,
		resolvers					++= Dependencies.resolutionRepos,
		scalacOptions				:= Seq("-feature", "-deprecation", "-encoding", "UTF8", "-unchecked"),
		testOptions in Test += Tests.Argument("-oDF"),
		version 					:= "latest"
	)

	lazy val dockerStuff = Seq(
		maintainer := "Ben Steer <b.a.steer@qmul.ac.uk>",
		dockerBaseImage := "errordeveloper/oracle-jre",
    dockerRepository := Some("quay.io/miratepuffin"),
		dockerExposedPorts := Seq(2551,8080,2552)
		// test <<= test dependsOn (publishLocal in docker)
		)

	lazy val root = Project(id = "dockerexp",
		base = file(".")) aggregate(cluster)

	lazy val cluster = project.in(file("cluster"))
		//.enablePlugins(JavaAppPackaging)
		.enablePlugins(AshScriptPlugin)
		.settings(isSnapshot := true)
		.settings(dockerStuff:_*)
		.settings(dockerEntrypoint := Seq("bin/cluster"))
		.settings(basicSettings: _*)
		.settings(libraryDependencies ++=
			dep_compile(
				typesafe_config, scalajack, akka_http, akka_streams, akka_actor, akka_cluster, akka_tools, akka_contrib, akka_remote, akka_slf4j, logback,spray_json,curator1,curator2) ++
			dep_test(scalatest)
		)
		.settings((Keys.test in Test) <<= (Keys.test in Test) dependsOn (publishLocal in Docker))
}
