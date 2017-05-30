package com.gwz.dockerexp

import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.{Await, Future}
import com.gwz.dockerexp.Actors.ClusterActors._
import com.gwz.dockerexp.caseclass.clustercase.{LogicNode, RestNode, SeedNode}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

//main function
object Go extends App {
	args(0) match {
		case "seed"   => SeedNode()
		case "rest"   => RestNode(args(1))
		case "logic"  => LogicNode(args(1))
	}
}






