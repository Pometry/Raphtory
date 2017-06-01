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
	}
}

//docker run -p 9101:2551 --rm -e "HOST_IP=161.23.245.59" -e "HOST_PORT=9101" dockerexp/cluster seed
//docker run -p 9102:2551 -p 8080:8080 --rm -e "HOST_IP=161.23.245.59" -e "HOST_PORT=9102" dockerexp/cluster rest 161.23.245.59:9101
//docker run -p 9103:2551  --rm -e "HOST_IP=161.23.245.59" -e "HOST_PORT=9103" dockerexp/cluster PartitionManager 161.23.245.59:9101 1
//docker run -p 9104:2551  --rm -e "HOST_IP=161.23.245.59" -e "HOST_PORT=9104" dockerexp/cluster Router 161.23.245.59:9101

