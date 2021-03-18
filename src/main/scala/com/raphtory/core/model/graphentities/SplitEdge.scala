package com.raphtory.core.model.graphentities

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object SplitEdge {
  def apply(
      workerID: Int,
      creationTime: Long,
      srcID: Long,
      dstID: Long,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property]
  ) = {
    val e = new SplitEdge(workerID: Int, creationTime, srcID, dstID, initialValue = true)
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
class SplitEdge(
    workerID: Int,
    msgTime: Long,
    srcID: Long,
    dstID: Long,
    initialValue: Boolean)
  extends Edge(workerID, msgTime, srcID, dstID, initialValue) {
}
