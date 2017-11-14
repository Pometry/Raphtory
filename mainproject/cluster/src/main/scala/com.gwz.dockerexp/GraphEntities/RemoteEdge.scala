package com.gwz.dockerexp.GraphEntities
/**
  * Extenetion of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  */
class RemoteEdge(msgID:Int,initialValue:Boolean,srcId:Int,dstId:Int,srcOrDst:RemotePos.Value,remoteID:Int) extends Edge(msgID,initialValue,srcId,dstId){
  val remotePos:RemotePos.Value =srcOrDst
  val remotePartitionID:Int = remoteID
  var count =0;
}

