package com.raphtory.GraphEntities

import com.raphtory.utils.Utils

import scala.collection.mutable

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
