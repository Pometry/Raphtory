package com.raphtory.caseclass

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.raphtory.utils.Utils
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

trait DocSvr {

  def seedLoc : String
  implicit val system : ActorSystem
  val clusterSystemName = Utils.clusterSystemName
  val ssn               = java.util.UUID.randomUUID.toString

  def init(seeds:List[String]) : ActorSystem = {

    val config = ConfigFactory.load()
        .withValue("akka.cluster.seed-nodes",
          ConfigValueFactory.fromIterable(
            JavaConversions.asJavaIterable(
              seeds.map(_ => s"akka.tcp://$clusterSystemName@$seedLoc").toIterable)
          )
        )

    val actorSystem = ActorSystem(clusterSystemName, config)

    printConfigInfo(config, actorSystem)

    actorSystem // return actor system with set config
  }

  var nodes = Set.empty[akka.cluster.Member]  // cluster node registry

  def getNodes() = nodes.map( m => s"${m.address} ${m.getRoles.mkString}").mkString(",")

  def printConfigInfo(config:Config, as:ActorSystem) ={
    println(s"------ $ssn ------")
    println(s"Binding core internally on ${config.getString("akka.remote.netty.tcp.bind-hostname")} port ${config.getString("akka.remote.netty.tcp.bind-port")}")
    println(s"Binding core externally on ${config.getString("akka.remote.netty.tcp.hostname")} port ${config.getString("akka.remote.netty.tcp.port")}")
    println(s"Seeds: ${config.getList("akka.cluster.seed-nodes").toList}")
    println(s"Roles: ${config.getList("akka.cluster.roles").toList}")
    println(s"My Akka URI: ${as.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress}")  // warning: unorthodox mechanism
  }
}
