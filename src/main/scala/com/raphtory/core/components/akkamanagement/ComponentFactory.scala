package com.raphtory.core.components.akkamanagement

import akka.actor.{ActorRef, ActorSystem, Props}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.raphtory.core.components.akkamanagement.connectors._
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.leader.{WatchDog, WatermarkManager}
import com.raphtory.core.components.spout.Spout
import com.rits.cloning.Cloner
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import java.lang.invoke.SerializedLambda
import scala.collection.JavaConversions

object ComponentFactory {

  val clusterSystemName = "Raphtory"

  val cloner = new Cloner
  def newGraphBuilder(gb:GraphBuilder[Any])={
    cloner.deepClone(gb).asInstanceOf[GraphBuilder[Any]]
  }

  def leader(address:String,port: Int,conf:Config = ConfigFactory.load()): (ActorRef, ActorRef) = {
    val leaderLoc = address+":"+port
    println(s"Creating leader at $leaderLoc")
    val system: ActorSystem = initialiseActorSystem(seeds = List(leaderLoc), port)
    val watchDog = system.actorOf(Props(new WatchDog()), "WatchDog")
    (system.actorOf(Props(new WatermarkManager(watchDog)), "WatermarkManager"), watchDog)
  }

  def builder(seed: String, port: Int, graphbuilder: GraphBuilder[Any]): ActorRef = {
    println("Creating Graph Builder")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new BuilderConnector(newGraphBuilder(graphbuilder))), "Builder")
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

  def query(seed: String, port: Int): ActorRef = {
    println("Creating Query Manager")
    val system: ActorSystem = initialiseActorSystem(seeds = List(seed), port)
    system.actorOf(Props(new QueryManagerConnector()), "QueryManagerConnector")
  }


  def initialiseActorSystem(seeds: List[String], port: Int): ActorSystem = {
    var config = ConfigFactory.load()
    val seedLoc = seeds.head
    val IP = config.getString("Raphtory.bindAddress")

    config = config.withValue(
      "akka.cluster.seed-nodes",
      ConfigValueFactory.fromIterable(
        JavaConversions.asJavaIterable(
          seeds.map(_ => s"akka://$clusterSystemName@$seedLoc")
        )
      )
    )
    config = config.withValue("akka.remote.artery.canonical.bind-port", ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(port))
    config = config.withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(IP))
    ActorSystem(clusterSystemName, config)
  }

}
