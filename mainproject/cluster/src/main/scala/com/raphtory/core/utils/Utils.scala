package com.raphtory.core.utils

import akka.actor.ActorContext
import com.raphtory.core.model.graphentities.{Edge, RemoteEdge, RemotePos}
import com.typesafe.config.ConfigFactory

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object Utils {
  val clusterSystemName = "dockerexp"
  val config            = ConfigFactory.load
  val partitionsTopic   = "/partitionsCount"
  val readersTopic      = "/readers"
  val liveAnalysisTopic      = "/liveanalysis"

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
  def edgeCreator(routerID: Int, msgId : Int, srcId: Int, dstId: Int, managerCount : Int, managerId : Int,
                  edges : TrieMap[Long, Edge], initialValue : Boolean = true, addOnly:Boolean) : (Edge, Boolean, Boolean) = {

    val local       = checkDst(dstId, managerCount, managerId)
    var present     = false
    var edge : Edge = null

    if (local)
      edge = new Edge(routerID, msgId, srcId, dstId, initialValue, addOnly)
    else
      edge = new RemoteEdge(routerID, msgId, srcId, dstId, initialValue, addOnly, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.putIfAbsent(getEdgeIndex(srcId, dstId), edge) match {
      case Some(e) => {
        edge = e
        present = true
      }
      case None => // All is set
    }
    (edge, local, present)
  }

  def getPartition(ID:Int, managerCount : Int):Int = ID % managerCount //get the partition a vertex is stored in
  def checkDst(dstID:Int, managerCount:Int, managerID:Int):Boolean = (dstID % managerCount) == managerID //check if destination is also local
  def getManager(srcId:Int, managerCount : Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment
    /**
    * Shifter to get only one Long as Edges indexing
    * @param srcId
    * @param dstId
    * @return
    */
  def getEdgeIndex(srcId : Int, dstId : Int): Long = {
    (srcId.toLong << 32) + dstId
  }

  /**
    * Get lowest (32 bit) part of a Binary string representing two Int numbers (used for Edges indexing) - The dstId of a given edgeId
    * @param index the Long representing the two Int numbers
    * @return the rightmost 32bit as an Int
    */
  def getIndexLO(index : Long) : Int = {
    ((index << 32) >> 32).toInt
  }

  /**
    * Get highest (32 bit) part of a binary string representing two Int number (used for Edges indexing)
    * @param index
    * @return the leftmost 32 bit as an Int
    */
  def getIndexHI(index : Long) : Int = {
    (index >> 32).toInt
  }


  def createHistory(history: mutable.TreeMap[Long, Boolean]):String = {
    if(history.size==0){
      return ""
    }
    var s = "{"
    for((k,v) <- history){
      s = s+ s"$k : $v, "
    }
    s.dropRight(2) + "}"
  }

  def createPropHistory(history: mutable.TreeMap[Long, String]):String = {
    if(history.size==0){
      return ""
    }
    var s = "{"
    for((k,v) <- history){
      s = s+ s"$k : '$v', "
    }
    s.dropRight(2) + "}"

  }
}
