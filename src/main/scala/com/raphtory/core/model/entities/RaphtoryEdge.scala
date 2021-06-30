package com.raphtory.core.model.entities

import com.raphtory.core.actors.partitionmanager.workers.ParquetEdge
import com.raphtory.core.model.EntityStorage

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object RaphtoryEdge {
  def apply(
      workerID: Int,
      creationTime: Long,
      srcID: Long,
      dstID: Long,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property],
      storage: EntityStorage
  ) = {

    val e = new RaphtoryEdge(workerID, creationTime, srcID, dstID, initialValue = true)
    e.history = previousState
    e.properties = properties
    e
  }

}

/**
  * Created by Mirate on 01/03/2017.
  */
class RaphtoryEdge(workerID: Int, msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends RaphtoryEntity(msgTime, initialValue) {

  def killList(vKills: List[(Long, Boolean)]): Unit = history ++= vKills

  def getSrcId: Long   = srcId
  def getDstId: Long   = dstId
  def getWorkerID: Int = workerID
  def serialise(): ParquetEdge = ParquetEdge(srcId,dstId,history.toList,properties.map(x=> x._2.serialise(x._1)).toList)
}
