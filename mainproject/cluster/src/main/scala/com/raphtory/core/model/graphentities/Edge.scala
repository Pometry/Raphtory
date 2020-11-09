package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object Edge {
  def apply(
      workerID: Int,
      creationTime: Long,
      srcID: Long,
      dstID: Long,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property],
      storage: EntityStorage
  ) = {

    val e = new Edge(workerID, creationTime, srcID, dstID, initialValue = true)
    e.history = previousState
    e.properties = properties
    e
  }

}

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(workerID: Int, msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends Entity(msgTime, initialValue) {

  def killList(vKills: mutable.TreeMap[Long, Boolean]): Unit = history ++= vKills

  def getSrcId: Long   = srcId
  def getDstId: Long   = dstId
  def getWorkerID: Int = workerID

  def viewAt(time: Long): Edge = {
    var closestTime: Long = 0
    var value             = false
    for ((k, v) <- history)
      if (k <= time)
        if ((time - k) < (time - closestTime)) {
          closestTime = k
          value = v
        }
    val edge = new Edge(-1, closestTime, srcId, dstId, value)
    for ((k, p) <- properties) {
      val value = p.valueAt(time)
      if (!(value equals ("")))
        edge + (time, false, k, value)
    }
    edge
  }


}
