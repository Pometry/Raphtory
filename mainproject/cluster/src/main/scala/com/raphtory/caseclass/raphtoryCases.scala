package com.raphtory.caseclass

import com.raphtory.Actors.RaphtoryActors.Analaysis.Analyser

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */

case class RouterUp(id:Int)
case class PartitionUp(id:Int)
case class ClusterStatusRequest()
case class ClusterStatusResponse(clusterUp: Boolean)

//The following block are all case classes (commands) which the manager can handle
case class LiveAnalysis(name: String,analyser: Analyser)
case class Results(result:Object)

case class VertexAdd(msgId:Int,srcId:Int) //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgId:Int,srcId:Int, properties: Map[String,String])
case class VertexUpdateProperties(msgId:Int,srcId:Int, propery:Map[String,String])
case class VertexRemoval(msgId:Int,srcId:Int)

case class EdgeAdd(msgId:Int,srcId:Int,dstId:Int)
case class EdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int, properties: Map[String,String])
case class EdgeUpdateProperties(msgId:Int,srcId:Int,dstId:Int,property:Map[String,String])
case class EdgeRemoval(msgId:Int,srcId:Int,dstID:Int)

case class RemoteEdgeUpdateProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String])
case class RemoteEdgeAdd(msgId:Int, srcId:Int, dstId:Int, properties: Map[String,String])
case class RemoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int)

case class RemoteEdgeUpdatePropertiesNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],kills:mutable.TreeMap[Int, Boolean])
case class RemoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,properties: Map[String,String],kills:mutable.TreeMap[Int, Boolean])
case class RemoteEdgeRemovalNew(msgId:Int,srcId:Int,dstId:Int,kills:mutable.TreeMap[Int, Boolean])

case class RemoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,kills:mutable.TreeMap[Int, Boolean])
case class ReturnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int)

case class UpdatedCounter(newValue : Int)
case class AssignedId(id : Int)
case class PartitionsCount(count : Int)
case class RequestPartitionId()
case class RequestRouterId()
//case class WatchDogIp(ip: String)

