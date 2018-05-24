package com.raphtory.core.model.communication

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.utils.CommandEnum

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait RaphCaseClass {
  def srcId:Int
}

case class Command(command: CommandEnum.Value, value: RaphCaseClass)

case class RouterUp(id:Int)
case class PartitionUp(id:Int)
case class ClusterStatusRequest()
case class ClusterStatusResponse(clusterUp: Boolean)

//The following block are all case classes (commands) which the manager can handle
case class LiveAnalysis(analyser: Analyser)
case class Results(result:Object)

case class VertexAdd(msgTime:Long, override val srcId:Int) extends RaphCaseClass //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgTime:Long, override val srcId:Int, properties: Map[String,String]) extends RaphCaseClass
case class VertexUpdateProperties(msgTime:Long,srcId:Int, propery:Map[String,String])
case class VertexRemoval(msgTime:Long,srcId:Int)

case class EdgeAdd(msgTime:Long,srcId:Int,dstId:Int)
case class EdgeAddWithProperties(msgTime:Long, override val srcId:Int,dstId:Int, properties: Map[String,String]) extends RaphCaseClass
case class EdgeUpdateProperties(msgTime:Long,srcId:Int,dstId:Int,property:Map[String,String])
case class EdgeRemoval(msgTime:Long,srcId:Int,dstID:Int)
case class EdgeUpdateProperty(msgTime : Long, edgeId : Long, key : String, value : String)
case class RemoteEdgeUpdateProperties(msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String])
case class RemoteEdgeAdd(msgTime:Long, srcId:Int, dstId:Int, properties: Map[String,String])
case class RemoteEdgeRemoval(msgTime:Long,srcId:Int,dstId:Int)

case class RemoteEdgeUpdatePropertiesNew(msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeAddNew(msgTime:Long,srcId:Int,dstId:Int,properties: Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeRemovalNew(msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean])

case class RemoteReturnDeaths(msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean])
case class ReturnEdgeRemoval(msgTime:Long,srcId:Int,dstId:Int)

case class UpdatedCounter(newValue : Int)
case class AssignedId(id : Int)
case class PartitionsCount(count : Int)
case class RequestPartitionId()
case class RequestRouterId()


sealed trait RaphReadClasses

case class Setup(analyzer : Analyser) extends RaphReadClasses
case class Ready() extends RaphReadClasses
case class NextStep(analyzer : Analyser) extends RaphReadClasses
case class EndStep(results : Any) extends RaphReadClasses // TODO Define results
case class GetNetworkSize() extends RaphReadClasses
case class NetworkSize(size : Int) extends RaphReadClasses


case class ClassMissing() extends RaphReadClasses
case class SetupNewAnalyser(analyser: String, name:String) extends RaphReadClasses
case class FailedToCompile (stackTrace:String) extends  RaphReadClasses
case class NextStepNewAnalyser(name: String) extends RaphReadClasses


//case class WatchDogIp(ip: String)

