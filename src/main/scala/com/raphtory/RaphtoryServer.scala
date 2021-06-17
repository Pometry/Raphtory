package com.raphtory

import java.lang.management.ManagementFactory
import java.net.InetAddress

import akka.actor.{ActorSystem, Address, ExtendedActorSystem, Props}
import akka.event.LoggingAdapter
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.clustermanagement.componentconnector.{PartitionConnector, RouterConnector}
import com.raphtory.core.actors.clustermanagement.{SeedActor, WatchDog, WatermarkManager}
import com.raphtory.core.actors.graphbuilder.GraphBuilder
import com.raphtory.core.actors.spout.{Spout, SpoutAgent}
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.sys.process._
//main function

object RaphtoryServer extends App {
//  Kamon.init() //start tool logging
  printJavaOptions()
  val conf    = ConfigFactory.load()
  val clusterSystemName  = "Raphtory"
  val ssn: String               = java.util.UUID.randomUUID.toString

  val partitionCount = sys.env.getOrElse("PARTITION_MIN","1").toInt
  val routerCount = sys.env.getOrElse("ROUTER_MIN","1").toInt
  val docker = System.getenv().getOrDefault("DOCKER", "false").trim.toBoolean

  args(0) match {
    case "seedNode" => seedNode()
    case "router" => router()
    case "partitionManager" => partition()
    case "spout" => spout()
    case "analysisManager" =>analysis()
    case "local" => local()
  }

  def seedNode() = {
    val seedLoc = s"${sys.env.getOrElse("HOST_IP","127.0.0.1")}:${conf.getInt("settings.bport")}"
    println(s"Creating seed node and watchdog at $seedLoc")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
    system.actorOf(Props(new SeedActor()), "cluster")
    system.actorOf(Props(new WatermarkManager(managerCount = partitionCount)),"WatermarkManager")
    system.actorOf(Props(new WatchDog(managerCount = partitionCount, minimumRouters = routerCount)), "WatchDog")
  }

  def router() = {
    println("Creating Router")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(locateSeed()))

    val builderPath  = s"${sys.env.getOrElse("GRAPHBUILDER", "")}"
    val graphBuilder = Class.forName(builderPath).getConstructor().newInstance().asInstanceOf[GraphBuilder[Any]]
    system.actorOf(Props(new RouterConnector(partitionCount, routerCount,graphBuilder)), "Routers")
  }

  def partition() = {
    println(s"Creating Partition Manager...")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(locateSeed()))
    system.actorOf(Props(new PartitionConnector(partitionCount,routerCount)), "PartitionManager")
  }

  def spout() = {
    println("Creating Update Generator")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(locateSeed()))
    val spoutPath = s"${sys.env.getOrElse("SPOUT", "")}"
    val spout = Class.forName(spoutPath).getConstructor().newInstance().asInstanceOf[Spout[Any]]
    system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  }

  def analysis() = {
    println("Creating Analysis Manager")
    implicit val system: ActorSystem = initialiseActorSystem(List(locateSeed()))
    system.actorOf(Props[AnalysisManager].withDispatcher("misc-dispatcher"), s"AnalysisManager")
    AnalysisRestApi(system)
  }

  def local() = {
    println("putting up cluster in one node")
    val spoutPath = s"${sys.env.getOrElse("SPOUT", "")}"
    val builderPath  = s"${sys.env.getOrElse("GRAPHBUILDER", "")}"
    RaphtoryGraph[Any](spoutPath,builderPath)
  }

  def locateSeed(): String =
    if (docker) {
     // while (!("nc -l seedNode -p 1600 -w 1" !).equals(0)) {
     //   println("Waiting for seednode to come online.")
        Thread.sleep(20000)
      //}
      println("Seed Found!")
      InetAddress.getByName("seedNode").getHostAddress() + ":1600"
    } else "127.0.0.1:1600"



  /** Initialise a new ActorSystem with configured name and seed nods
    *
    * @param seeds the set of Seed nodes to be added to the System
    * @return A new Akka ActorSystem object with the set config and seed nodes
    *         as determined by the ${seeds} parameter
    */
  def initialiseActorSystem(seeds: List[String]): ActorSystem = {
    var config = ConfigFactory.load()
    val seedLoc = seeds.head
    if (docker)
      config = config.withValue(
        "akka.cluster.seed-nodes",
        ConfigValueFactory.fromIterable(
          JavaConversions.asJavaIterable(
            seeds.map(_ => s"akka://$clusterSystemName@$seedLoc")
          )
        )
      )

    val actorSystem = ActorSystem(clusterSystemName, config)
    if (!docker) {
      AkkaManagement.get(actorSystem).start()
      ClusterBootstrap.get(actorSystem).start()
    }
    printConfigInfo(config, actorSystem)
    actorSystem
  }



  case class SocketAddress(host: String, port: String)
  case class SystemConfig(bindAddress: SocketAddress, tcpAddress: SocketAddress, seeds: List[ConfigValue], roles: List[ConfigValue])
  def parseConfig(config: Config): SystemConfig = {
    val bindHost    = config.getString("akka.remote.artery.canonical.bind-hostname")
    val bindPort    = config.getString("akka.remote.artery.canonical.bind-port")
    val bindAddress = SocketAddress(bindHost, bindPort)

    val tcpHost    = config.getString("akka.remote.artery.canonical.hostname")
    val tcpPort    = config.getString("akka.remote.artery.canonical.port")
    val tcpAddress = SocketAddress(tcpHost, tcpPort)

    val seeds = config.getList("akka.cluster.seed-nodes").toList
    val roles = config.getList("akka.cluster.roles").toList

    SystemConfig(bindAddress = bindAddress, tcpAddress = tcpAddress, seeds, roles)
  }

  /** Utility method to print the configuration which an ActorSystem has been created under
    *
    * @param config a TypeSafe config object detailing the Akka system configuration
    * @param system an initialised ActorSystem object
    */
  def printConfigInfo(config: Config, system: ActorSystem): Unit = {
    val log: LoggingAdapter = system.log

    val systemConfig: SystemConfig = parseConfig(config)
    val bindAddress: SocketAddress = systemConfig.bindAddress
    val tcpAddress: SocketAddress  = systemConfig.tcpAddress

    log.info(s"Created ActorSystem with ID: $ssn")

    log.info(s"Binding ActorSystem internally to address ${bindAddress.host}:${bindAddress.port}")
    log.info(s"Binding ActorSystem externally to host ${tcpAddress.host}:${tcpAddress.port}")

    log.info(s"Registering the following seeds to ActorSystem: ${systemConfig.seeds}")
    log.info(s"Registering the following roles to ActorSystem: ${systemConfig.roles}")

    // FIXME: This is bit unorthodox ...
    val akkaSystemUrl: Address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    log.info(s"ActorSystem successfully initialised at the following Akka URL: $akkaSystemUrl")
  }

  def printJavaOptions(): Unit = println(s"Current java options: ${ManagementFactory.getRuntimeMXBean.getInputArguments}")

}
