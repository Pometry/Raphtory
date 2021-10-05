package com.raphtory.core.components.akkamanagement

import akka.actor.{ActorRef, ActorSystem, Props}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.raphtory.core.components.akkamanagement.connectors._
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.leader.{WatchDog, WatermarkManager}
import com.raphtory.core.components.spout.Spout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import java.lang.invoke.SerializedLambda
import scala.collection.JavaConversions

object ComponentFactory {
  val conf = ConfigFactory.load()
  val clusterSystemName = "Raphtory"


  def leader(port: Int): (ActorRef, ActorRef) = {
    val address = s"127.0.0.1:$port"
    println(s"Creating leader at $address")
    val system: ActorSystem = initialiseActorSystem(seeds = List(address), port)
    val watchDog = system.actorOf(Props(new WatchDog()), "WatchDog")
    (system.actorOf(Props(new WatermarkManager(watchDog)), "WatermarkManager"), watchDog)
  }

  def builder(seed: String, port: Int, graphbuilder: GraphBuilder[Any]): ActorRef = {
    println("Creating Graph Builder")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new BuilderConnector(graphbuilder)), "Builder")
  }

  def partition(seed: String, port: Int): ActorRef = {
    println(s"Creating Partition Manager...")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new PartitionConnector()), "PartitionManager")
  }

  def spout(seed: String, port: Int, spout: Spout[Any]): ActorRef = {
    println("Creating Update Generator")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new SpoutConnector(spout)), "SpoutConnector")
  }

  def analysis(seed: String, port: Int): ActorRef = {
    println("Creating Analysis Manager")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new AnalysisManagerConnector()), "AnalysisManagerConnector")
  }

  def query(seed: String, port: Int): ActorRef = {
    println("Creating Query Manager")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new QueryManagerConnector()), "AnalysisManagerConnector")
  }


  def initialiseActorSystem(seeds: List[String], port: Int): ActorSystem = {
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
    println(config.getString("akka-kryo-serialization.kryo-initializer"))
    config = config.withValue("akka.remote.artery.canonical.bind-port", ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef("127.0.0.1"))
    ActorSystem(clusterSystemName, config)
  }

}
