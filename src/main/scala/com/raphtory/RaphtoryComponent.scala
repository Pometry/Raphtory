package com.raphtory
import akka.actor.{ActorRef, ActorSystem, Props}
import com.esotericsoftware.kryo.Kryo
import com.raphtory.core.actors.orchestration.componentconnector.{AnalysisManagerConnector, PartitionConnector, BuilderConnector, SpoutConnector}
import com.raphtory.core.actors.graphbuilder.GraphBuilder
import com.raphtory.core.actors.orchestration.clustermanager.{SeedActor, WatchDog, WatermarkManager}
import com.raphtory.core.actors.spout.{Spout, SpoutAgent}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConversions
import scala.language.postfixOps
//main function
class RaphtoryComponent(component:String,port:Int,classPath:String="") {
  val conf    = ConfigFactory.load()
  val clusterSystemName  = "Raphtory"
  val kryo = new Kryo()
  kryo.register(Array[Tuple2[Long,Boolean]]().getClass,3000)
  kryo.register(classOf[Array[scala.Tuple2[Long,Boolean]]],3001)
  kryo.register(scala.Tuple2.getClass,3002)
  kryo.register(classOf[scala.Tuple2[Long,Boolean]],3003)

  private var seedNodeRef:ActorRef = _
  private var watermarkerRef:ActorRef = _
  private var watchdogRef:ActorRef = _
  private var builderRef:ActorRef = _
  private var partitionManagerRef:ActorRef = _
  private var spoutRef:ActorRef = _
  private var analysisManagerRef:ActorRef = _

  def getSeedNode:Option[ActorRef] = Option(seedNodeRef)
  def getWatermarker:Option[ActorRef] = Option(watermarkerRef)
  def getWatchdog:Option[ActorRef] = Option(watchdogRef)
  def getBuilder:Option[ActorRef] = Option(builderRef)
  def getPartition:Option[ActorRef] = Option(partitionManagerRef)
  def getSpout:Option[ActorRef] = Option(spoutRef)
  def getAnalysisManager:Option[ActorRef] = Option(analysisManagerRef)


  component match {
    case "seedNode" => seedNode()
    case "builder" => builder()
    case "partitionManager" => partition()
    case "spout" => spout()
    case "analysisManager" =>analysis()
  }


  def seedNode() = {
    val seedLoc = s"127.0.0.1:$port"
    println(s"Creating seed node and watchdog at $seedLoc")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
    seedNodeRef = system.actorOf(Props(new SeedActor()), "cluster")
    watermarkerRef = system.actorOf(Props(new WatermarkManager()),"WatermarkManager")
    watchdogRef = system.actorOf(Props(new WatchDog()), "WatchDog")
  }

  def builder() = {
    println("Creating Graph Builder")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    val graphBuilder = Class.forName(classPath).getConstructor().newInstance().asInstanceOf[GraphBuilder[Any]]
    builderRef =system.actorOf(Props(new BuilderConnector(graphBuilder)), "Builder")
  }

  def partition() = {
    println(s"Creating Partition Manager...")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    partitionManagerRef = system.actorOf(Props(new PartitionConnector()), "PartitionManager")
  }

  def spout() = {
    println("Creating Update Generator")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    val spout = Class.forName(classPath).getConstructor().newInstance().asInstanceOf[Spout[Any]]
    spoutRef = system.actorOf(Props(new SpoutConnector(spout)), "SpoutConnector")
  }

  def analysis() = {
    println("Creating Analysis Manager")
    implicit val system: ActorSystem = initialiseActorSystem(seeds = List("127.0.0.1:1600"))
    analysisManagerRef = system.actorOf(Props(new AnalysisManagerConnector()), "AnalysisManagerConnector")

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

}
















