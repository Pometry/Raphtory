package com.raphtory.internals.storage.pojograph.entities.internal

/** Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  */
private[raphtory] class SplitEdge(msgTime: Long, index: Long, srcID: Long, dstID: Long, initialValue: Boolean)
        extends PojoEdge(msgTime, index, srcID, dstID, initialValue) {}
