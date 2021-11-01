package com.raphtory.core.implementations.chroniclegraph.entities.internal

/** *
  * Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  *
  */
class SplitEdge(msgTime: Long, srcID: Long, dstID: Long, initialValue: Boolean) extends ChronicleEdge(msgTime, srcID, dstID, initialValue) {
}
