package com.raphtory.GraphEntities

/** *
  * Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  *
  * @param msgTime
  * @param initialValue
  * @param srcId
  * @param dstId
  * @param remotePos
  * @param remotePartitionID
  */
case class RemoteEdge(msgTime: Long,
                      srcId: Int,
                      dstId: Int,
                      initialValue: Boolean,
                      addOnly:Boolean,
                      remotePos: RemotePos.Value,
                      remotePartitionID: Int)
    extends Edge(msgTime, srcId, dstId, initialValue, addOnly)
