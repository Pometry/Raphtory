package com.gwz.dockerexp.GraphEntities

/** *
  * Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  *
  * @param msgID
  * @param initialValue
  * @param srcId
  * @param dstId
  * @param remotePos
  * @param remotePartitionID
  */
case class RemoteEdge(msgID: Int,
                      initialValue: Boolean,
                      srcId: Int,
                      dstId: Int,
                      remotePos: RemotePos.Value,
                      remotePartitionID: Int)
    extends Edge(msgID, initialValue, srcId, dstId)
