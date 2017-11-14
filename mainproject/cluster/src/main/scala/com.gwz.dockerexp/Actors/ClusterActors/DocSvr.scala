package com.gwz.dockerexp.Actors.ClusterActors

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.{Await, Future}
import com.gwz.dockerexp.Actors.ClusterActors._
import com.gwz.dockerexp.IpAndPort
import com.gwz.dockerexp.caseclass.clustercase.{LogicNode, RestNode, SeedNode}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

trait DocSvr {
  val clusterSystemName = "dockerexp"
  val ssn = java.util.UUID.randomUUID.toString
  val ipAndPort = IpAndPort()
  implicit val system:ActorSystem

  def init( seeds:List[String] ) : ActorSystem = {
    val config = ConfigFactory.load()
      .withValue("akka.remote.netty.tcp.bind-hostname", ConfigValueFactory.fromAnyRef(java.net.InetAddress.getLocalHost().getHostAddress()))
      .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(ipAndPort.hostIP))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(ipAndPort.akkaPort))
      .withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromIterable(JavaConversions.asJavaIterable(seeds.map(seedLoc => s"akka.tcp://$clusterSystemName@$seedLoc").toIterable)))

    val actorSystem = ActorSystem( clusterSystemName, config )
    printConfigInfo(config,actorSystem)
    actorSystem // return actor system with set config
  }

  var nodes = Set.empty[akka.cluster.Member]  // cluster node registry
  def getNodes() = nodes.map( m => m.address+" "+m.getRoles.mkString ).mkString(",")
  def printConfigInfo(config:Config, as:ActorSystem) ={
    println(s"------ $ssn ------")
    println(s"Binding core internally on ${config.getString("akka.remote.netty.tcp.bind-hostname")} port ${config.getString("akka.remote.netty.tcp.bind-port")}")
    println(s"Binding core externally on ${config.getString("akka.remote.netty.tcp.hostname")} port ${config.getString("akka.remote.netty.tcp.port")}")
    println(s"Seeds: ${config.getList("akka.cluster.seed-nodes").toList}")
    println("Roles: "+config.getList("akka.cluster.roles").toList)
    println("My Akka URI: " + as.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)  // warning: unorthodox mechanism
  }
}
