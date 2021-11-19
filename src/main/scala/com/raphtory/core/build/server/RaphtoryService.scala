package com.raphtory.core.build.server

import com.raphtory.core.components.akkamanagement.ComponentFactory
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.typesafe.config.ConfigFactory

abstract class RaphtoryService[T] {
  def defineSpout():Spout[T]
  def defineBuilder: GraphBuilder[T]

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
    val leaderAddress  = config.getString("Raphtory.leaderAddress")
    val leaderPort  = config.getString("Raphtory.leaderPort").toInt
    ComponentFactory.leader(leaderAddress,leaderPort)
  }

  def spoutDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderAddress  = config.getString("Raphtory.leaderAddress")
    val leaderPort  = config.getString("Raphtory.leaderPort").toInt
    val leaderLoc = leaderAddress+":"+leaderPort
    val port = config.getInt("Raphtory.bindPort")
    ComponentFactory.spout(leaderLoc,port,defineSpout)
  }

  def builderDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderAddress  = config.getString("Raphtory.leaderAddress")
    val leaderPort  = config.getString("Raphtory.leaderPort").toInt
    val leaderLoc = leaderAddress+":"+leaderPort
    val port = config.getInt("Raphtory.bindPort")
    ComponentFactory.builder(leaderLoc,port,defineBuilder.asInstanceOf[GraphBuilder[Any]])
  }

  def partitionDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderAddress  = config.getString("Raphtory.leaderAddress")
    val leaderPort  = config.getString("Raphtory.leaderPort").toInt
    val leaderLoc = leaderAddress+":"+leaderPort
    val port = config.getInt("Raphtory.bindPort")
    ComponentFactory.partition(leaderLoc,port)
  }

  def queryManagerDeploy(): Unit = {
    val config = ConfigFactory.load()
    val leaderAddress  = config.getString("Raphtory.leaderAddress")
    val leaderPort  = config.getString("Raphtory.leaderPort").toInt
    val leaderLoc = leaderAddress+":"+leaderPort
    val port = config.getInt("Raphtory.bindPort")
    ComponentFactory.query(leaderLoc,port)
  }






}
