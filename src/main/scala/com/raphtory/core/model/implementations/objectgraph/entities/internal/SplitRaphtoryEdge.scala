package com.raphtory.core.model.implementations.objectgraph.entities.internal

import com.raphtory.core.actors.partitionmanager.ParquetEdge

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object SplitRaphtoryEdge {
  def apply(
      creationTime: Long,
      srcID: Long,
      dstID: Long,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property]
  ) = {
    val e = new SplitRaphtoryEdge(creationTime, srcID, dstID, initialValue = true)
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
    msgTime: Long,
    srcID: Long,
    dstID: Long,
    initialValue: Boolean)
  extends RaphtoryEdge(msgTime, srcID, dstID, initialValue) {
  override def serialise(): ParquetEdge = ParquetEdge(srcID,dstID,true,history.toList,properties.map(x=> x._2.serialise(x._1)).toList)

}
