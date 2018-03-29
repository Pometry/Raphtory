package com.raphtory.utils

import java.net.InetAddress

import akka.actor.ActorContext
import com.typesafe.config.ConfigFactory

object Utils {
  val clusterSystemName = "dockerexp"
  val config            = ConfigFactory.load
  val partitionsTopic   = "/partitionsCount"

  def watchDogSelector(context : ActorContext, ip : String) = {
    // IP $clusterSystemName@${InetAddress.getByName("watchDog").getHostAddress()}
    context.actorSelection(s"akka.tcp://$ip:${config.getString("settings.bport")}/user/WatchDog")
  }

  def getManager(srcId:Int, managerCount : Int):String = s"/user/partitionManager/Manager_${srcId % managerCount}" //simple srcID hash at the moment
}
