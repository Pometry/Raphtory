package com.raphtory
import java.lang.management.ManagementFactory
import java.net.InetAddress

import akka.actor.{ActorSystem, Address, ExtendedActorSystem, Props}
import akka.event.LoggingAdapter
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import com.esotericsoftware.kryo.Kryo
import com.raphtory.core.actors.AnalysisManager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.ClusterManagement.{RaphtoryReplicator, SeedActor, WatchDog, WatermarkManager}
import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.actors.Spout.{Spout, SpoutAgent}
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.sys.process._
//main function
class RaphtoryComponent(component:String,partitionCount:Int,routerCount:Int,port:Int,classPath:String="") {
  val conf    = ConfigFactory.load()
  val clusterSystemName  = "Raphtory"
  val kryo = new Kryo()
  kryo.register(Array[Tuple2[Long,Boolean]]().getClass,3000)
  kryo.register(classOf[Array[scala.Tuple2[Long,Boolean]]],3001)
  kryo.register(scala.Tuple2.getClass,3002)
  kryo.register(classOf[scala.Tuple2[Long,Boolean]],3003)

  component match {
    case "seedNode" => seedNode()
    case "router" => router()
    case "partitionManager" => partition()
    case "spout" => spout()
    case "analysisManager" =>analysis()
    case "watchdog" => watchDog()
  }
  def seedNode() = {
    val seedLoc = s"127.0.0.1:$port"
    println(s"Creating seed node at $seedLoc")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
    system.actorOf(Props(new SeedActor()), "cluster")
  }
  def initialiseActorSystem(seeds: List[String]): ActorSystem = {
    var config = ConfigFactory.load()
    val seedLoc = seeds.head

    config = config.withValue(
      "akka.cluster.seed-nodes",
      ConfigValueFactory.fromIterable(
        JavaConversions.asJavaIterable(
          seeds.map(_ => s"akka://$clusterSystemName@$seedLoc")
        )
      )
    )

    config = config.withValue("akka.remote.artery.canonical.bind-port",ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.port",ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.hostname",ConfigValueFactory.fromAnyRef("127.0.0.1"))
    ActorSystem(clusterSystemName, config)
  }

  def router() = {
    println("Creating Router")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    val graphBuilder = Class.forName(classPath).getConstructor().newInstance().asInstanceOf[GraphBuilder[Any]]
    system.actorOf(Props(RaphtoryReplicator("Router", partitionCount, routerCount,graphBuilder)), "Routers")
  }

  def partition() = {
    println(s"Creating Partition Manager...")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    system.actorOf(Props(RaphtoryReplicator(actorType = "Partition Manager", initialManagerCount = partitionCount,initialRouterCount = routerCount)), "PartitionManager")
  }

  def spout() = {
    println("Creating Update Generator")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    val spout = Class.forName(classPath).getConstructor().newInstance().asInstanceOf[Spout[Any]]
    system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  }

  def analysis() = {
    println("Creating Analysis Manager")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    AnalysisRestApi(system)
    system.actorOf(Props[AnalysisManager].withDispatcher("misc-dispatcher"), s"AnalysisManager")
  }

  def watchDog() = {
    println("Cluster Up, informing Partition Managers and Routers")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    system.actorOf(Props(new WatermarkManager(managerCount = partitionCount)),"WatermarkManager")
    system.actorOf(Props(new WatchDog(managerCount = partitionCount, minimumRouters = routerCount)), "WatchDog")
  }
}
















