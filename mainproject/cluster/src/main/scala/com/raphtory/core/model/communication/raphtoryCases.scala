package com.raphtory.core.model.communication

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.utils.CommandEnum

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait RaphWriteClass {
  def srcId:Int
}

trait SpoutGoing

case class Command(command: CommandEnum.Value, value: RaphWriteClass)

case class RouterUp(id:Int)
case class PartitionUp(id:Int)
case class ClusterStatusRequest()
case class ClusterStatusResponse(clusterUp: Boolean)

//The following block are all case classes (commands) which the manager can handle
case class LiveAnalysis(analyser: Analyser)
case class Results(result:Object)

case class VertexAdd(msgTime:Long, override val srcId:Int) extends RaphWriteClass //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgTime:Long, override val srcId:Int, properties: Map[String,String]) extends RaphWriteClass
case class VertexUpdateProperties(msgTime:Long,override val srcId:Int, propery:Map[String,String]) extends  RaphWriteClass
case class VertexRemoval(msgTime:Long,override val srcId:Int) extends RaphWriteClass

case class EdgeAdd(msgTime:Long,srcId:Int,dstId:Int) extends RaphWriteClass
case class EdgeAddWithProperties(msgTime:Long, override val srcId:Int,dstId:Int, properties: Map[String,String]) extends RaphWriteClass
case class EdgeUpdateProperties(msgTime:Long,override val srcId:Int,dstId:Int,property:Map[String,String]) extends RaphWriteClass
case class EdgeRemoval(msgTime:Long,override val srcId:Int,dstID:Int) extends RaphWriteClass

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
case class RequestPartitionCount()
case class RequestPartitionId()
case class RequestRouterId()


sealed trait RaphReadClasses

case class AnalyserPresentCheck(classname:String) extends  RaphReadClasses
case class AnalyserPresent() extends  RaphReadClasses
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

