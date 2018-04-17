package com.raphtory.GraphEntities

import com.raphtory.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
/**
  * Companion Edge object (extended creator for storage loads)
  */
object Edge {
  def apply(creationTime : Long, edgeId : Long,
            previousState : mutable.TreeMap[Long, Boolean],
            properties : TrieMap[String, Property]) = {

    val srcId = Utils.getIndexHI(edgeId)
    val dstId = Utils.getIndexLO(edgeId)

    val e = new Edge(creationTime, srcId, dstId, initialValue = true, addOnly = false)
    e.previousState   = previousState
    e.properties      = properties
    e
  }
}

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(msgTime: Long, srcId: Int, dstId: Int, initialValue: Boolean, addOnly:Boolean)
    extends Entity(msgTime, initialValue,addOnly) {

  /*override def printProperties: String =
    s"Edge between $srcId and $dstId: with properties: " + System
      .lineSeparator() +
      super.printProperties()
  */

  def killList(vKills: mutable.TreeMap[Long, Boolean]): Unit = {
    removeList ++= vKills
    if (!addOnly)
      previousState ++= vKills
  }

  override def getId: Long = Utils.getEdgeIndex(srcId, dstId)
}
