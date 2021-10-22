package com.raphtory.core.build.server

import com.raphtory.core.components.akkamanagement.ComponentFactory
import com.raphtory.core.components.akkamanagement.RaphtoryActor.conf
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.typesafe.config.ConfigFactory

class DeploymentDefinition[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {


  def main(args: Array[String]): Unit = {

    args(0) match {
      case "leader" => leaderDeploy()
      case "spout" => spoutDeploy()
      case "builder" => builderDeploy()
      case "partitionManager" => partitionDeploy()
      case "queryManager" => queryManagerDeploy()
    }
  }

  def leaderDeploy(): Unit = {
    val config = ConfigFactory.load()
    val port = config.getInt("Raphtory.port")
    ComponentFactory.leader(port)
  }

  def spoutDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderLoc  = config.getString("Raphtory.leaderLoc")
    val port = config.getInt("Raphtory.port")
    ComponentFactory.spout(leaderLoc,port,spout)
  }

  def builderDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderLoc  = config.getString("Raphtory.leaderLoc")
    val port = config.getInt("Raphtory.port")
    ComponentFactory.builder(leaderLoc,port,graphBuilder.asInstanceOf[GraphBuilder[Any]])
  }

  def partitionDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderLoc  = config.getString("Raphtory.leaderLoc")
    val port = config.getInt("Raphtory.port")
    ComponentFactory.partition(leaderLoc,port)
  }

  def queryManagerDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderLoc  = config.getString("Raphtory.leaderLoc")
    val port = config.getInt("Raphtory.port")
    ComponentFactory.query(leaderLoc,port)

  }






}
