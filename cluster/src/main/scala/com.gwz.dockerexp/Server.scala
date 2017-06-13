package com.gwz.dockerexp

import akka.actor.Props
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.gwz.dockerexp.Actors.RaphtoryActors.{PartitionManager, RaphtoryRouter}

import scala.concurrent.{Await, Future}
import com.gwz.dockerexp.caseclass.clustercase._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

//main function
object Go extends App {
	args(0) match {
		case "seed"   => {
      println("Creating seed node")
      SeedNode()
    }
		case "rest"   => {
      println("Creating rest node")
      RestNode(args(1))
    }
    case "logic"   => {
      println("Creating logic node")
      LogicNode(args(1))
    }
    case "router" => {
      println("Creating Router")
      RouterNode(args(1),args(2))
    }
    case "partitionManager" => {
      println(s"Creating Patition Manager ID: ${args(2)}")
      ManagerNode(args(1),args(2),args(3))
    }

    case "updateGen" =>{
      println("Creating Update Generator")
      UpdateNode(args(1),args(2))
    }
	}
}

