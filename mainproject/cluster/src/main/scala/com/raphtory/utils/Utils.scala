package com.raphtory.utils

import akka.actor.ActorContext
import com.raphtory.GraphEntities.{Edge, RemoteEdge, RemotePos}
import com.typesafe.config.ConfigFactory

import scala.collection.concurrent.TrieMap

object Utils {
  val clusterSystemName = "dockerexp"
  val config            = ConfigFactory.load
  val partitionsTopic   = "/partitionsCount"

  def watchDogSelector(context : ActorContext, ip : String) = {
    // IP $clusterSystemName@${InetAddress.getByName("watchDog").getHostAddress()}
    context.actorSelection(s"akka.tcp://$ip:${config.getString("settings.bport")}/user/WatchDog")
  }

  /**
    *
    * @param msgId
    * @param srcId
    * @param dstId
    * @param managerCount
    * @param managerId
    * @param edges
    * @return (Edge, Local, Present)
    */
  def edgeCreator(msgId : Int, srcId: Int, dstId: Int, managerCount : Int, managerId : Int,
                  edges : TrieMap[(Int, Int), Edge], initialValue : Boolean = true) : (Edge, Boolean, Boolean) = {
    edges.get((srcId,dstId)) match {
      case Some(e) => {
        (e, e.isInstanceOf[Edge], true)
      }
      case None => {
        if (checkDst(dstId, managerCount, managerId)) {
          val edge = new Edge(msgId, initialValue, srcId, dstId)
          edges put((srcId,dstId),edge)
          (edge , true, false)
        }
        else {
          val edge = new RemoteEdge(msgId, initialValue, srcId, dstId, RemotePos.Destination, getPartition(dstId, managerCount))
          edges put((srcId, dstId), edge)
          (edge, false, false)
        }
      }
    }
  }

  def getPartition(ID:Int, managerCount : Int):Int = ID % managerCount //get the partition a vertex is stored in
  def checkDst(dstID:Int, managerCount:Int, managerID:Int):Boolean = (dstID % managerCount) == managerID //check if destination is also local
  def getManager(srcId:Int, managerCount : Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment
}
