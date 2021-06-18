package com.raphtory
import akka.actor.{ActorSystem, Props}
import com.esotericsoftware.kryo.Kryo
import com.raphtory.RaphtoryServer.{partitionCount, routerCount}
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.clustermanagement.componentconnector.{AnalysisManagerConnector, PartitionConnector, RouterConnector, SpoutConnector}
import com.raphtory.core.actors.clustermanagement.{SeedActor, WatchDog, WatermarkManager}
import com.raphtory.core.actors.graphbuilder.GraphBuilder
import com.raphtory.core.actors.spout.{Spout, SpoutAgent}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.language.postfixOps
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
  }
  def seedNode() = {
    val seedLoc = s"127.0.0.1:$port"
    println(s"Creating seed node and watchdog at $seedLoc")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
    system.actorOf(Props(new SeedActor()), "cluster")
    system.actorOf(Props(new WatermarkManager(managerCount = partitionCount)),"WatermarkManager")
    system.actorOf(Props(new WatchDog(managerCount = partitionCount, minimumRouters = routerCount)), "WatchDog")
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
    system.actorOf(Props(new RouterConnector(partitionCount, routerCount,graphBuilder)), "Routers")
  }

  def partition() = {
    println(s"Creating Partition Manager...")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    system.actorOf(Props(new PartitionConnector(partitionCount,routerCount)), "PartitionManager")
  }

  def spout() = {
    println("Creating Update Generator")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    val spout = Class.forName(classPath).getConstructor().newInstance().asInstanceOf[Spout[Any]]
    system.actorOf(Props(new SpoutConnector(partitionCount,routerCount,spout)), "SpoutConnector")
  }

  def analysis() = {
    println("Creating Analysis Manager")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    system.actorOf(Props(new AnalysisManagerConnector(partitionCount,routerCount)), "AnalysisManagerConnector")

  }

}
















