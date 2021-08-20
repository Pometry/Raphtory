package com.raphtory.core.model.entities

import com.raphtory.core.actors.partitionmanager.workers.ParquetEdge

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object SplitRaphtoryEdge {
  def apply(
      workerID: Int,
      creationTime: Long,
      srcID: Long,
      dstID: Long,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property]
  ) = {
    val e = new SplitRaphtoryEdge(workerID: Int, creationTime, srcID, dstID, initialValue = true)
    e.history = previousState
    e.properties = properties
    e
  }
}

/** *
  * Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  *
  */
class SplitRaphtoryEdge(
    workerID: Int,
    msgTime: Long,
    srcID: Long,
    dstID: Long,
    initialValue: Boolean)
  extends RaphtoryEdge(workerID, msgTime, srcID, dstID, initialValue) {
  override def serialise(): ParquetEdge = ParquetEdge(srcID,dstID,true,workerID,history.toList,properties.map(x=> x._2.serialise(x._1)).toList)

}
