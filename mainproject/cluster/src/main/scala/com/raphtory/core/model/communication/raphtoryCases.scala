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

case class VertexAdd(routerID:Int,msgTime:Long, override val srcId:Int) extends RaphWriteClass //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(routerID:Int,msgTime:Long, override val srcId:Int, properties: Map[String,String]) extends RaphWriteClass
case class VertexUpdateProperties(routerID:Int,msgTime:Long,override val srcId:Int, propery:Map[String,String]) extends  RaphWriteClass
case class VertexRemoval(routerID:Int,msgTime:Long,override val srcId:Int) extends RaphWriteClass

case class EdgeAdd(routerID:Int,msgTime:Long,srcId:Int,dstId:Int) extends RaphWriteClass
case class EdgeAddWithProperties(routerID:Int,msgTime:Long, override val srcId:Int,dstId:Int, properties: Map[String,String]) extends RaphWriteClass
case class EdgeUpdateProperties(routerID:Int,msgTime:Long,override val srcId:Int,dstId:Int,property:Map[String,String]) extends RaphWriteClass
case class EdgeRemoval(routerID:Int,msgTime:Long,override val srcId:Int,dstID:Int) extends RaphWriteClass

case class EdgeUpdateProperty(msgTime : Long, edgeId : Long, key : String, value : String) //for data coming from the LAM
case class RemoteEdgeUpdateProperties(routerID:Int,msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String])
case class RemoteEdgeAdd(routerID:Int,msgTime:Long, srcId:Int, dstId:Int, properties: Map[String,String])
case class RemoteEdgeRemoval(routerID:Int,msgTime:Long,srcId:Int,dstId:Int)

case class RemoteEdgeUpdatePropertiesNew(routerID:Int,msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeAddNew(routerID:Int,msgTime:Long,srcId:Int,dstId:Int,properties: Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeRemovalNew(routerID:Int,msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean])

case class RemoteReturnDeaths(msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean])
case class ReturnEdgeRemoval(routerID:Int,msgTime:Long,srcId:Int,dstId:Int)

//BLOCK FROM WORKER SYNC
case class DstAddForOtherWorker(routerID:Int,msgTime:Long,dstID:Int,srcForEdge:Int,present:Boolean)
case class DstWipeForOtherWorker(routerID:Int,msgTime:Long,dstID:Int,srcForEdge:Int,present:Boolean)
case class EdgeRemoveForOtherWorker(routerID:Int,msgTime:Long,srcID:Int,dstID:Int)

case class UpdatedCounter(newValue : Int)
case class AssignedId(id : Int)
case class PartitionsCount(count : Int)
case class PartitionsCountResponse(count:Int)
case class RequestPartitionCount()
case class RequestPartitionId()
case class RequestRouterId()

case class CompressEdges(lastSaved:Long)
case class CompressEdge(key:Long,time:Long)
case class CompressVertices(lastSaved:Long)
case class CompressVertex(key:Int,time:Long)
case class FinishedEdgeCompression(key:Long)
case class FinishedVertexCompression(key:Int)

case class ArchiveEdges(removalPoint:Long)
case class ArchiveEdge(key:Long,time:Long)
case class ArchiveVertices(removalPoint:Long)
case class ArchiveVertex(key:Int,time:Long)
case class FinishedEdgeArchiving(key:Long, archived:(Int,Int,Int))
case class FinishedVertexArchiving(key:Int,archived:(Int,Int,Int))

case class SetupSlave(children:Int)

case class ReportIntake(mainMessages:Int,secondaryMessages:Int,partitionId:Int)

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

case class AllocateJob(record:Any)

case class CheckVertex()// extends CheckingFunction
case class CheckEdges()// extends CheckingFunction

case class EdgeAvgTrait()
case class VertexAvgTrait()

case class EdgeAvg()
case class VertexAvg()
//sealed trait CheckingFunction
//case class WatchDogIp(ip: String)

