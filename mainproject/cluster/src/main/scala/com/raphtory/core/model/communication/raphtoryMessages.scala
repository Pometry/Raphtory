package com.raphtory.core.model.communication

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.graphentities.Edge
import com.raphtory.core.utils.CommandEnum

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait RaphWriteClass {
  def srcID:Long
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

case class VertexAdd(msgTime:Long, override val srcID:Long) extends RaphWriteClass //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgTime:Long, override val srcID:Long, properties: Map[String,String]) extends RaphWriteClass
case class VertexUpdateProperties(msgTime:Long,override val srcID:Long, propery:Map[String,String]) extends  RaphWriteClass
case class VertexRemoval(msgTime:Long,override val srcID:Long) extends RaphWriteClass

case class EdgeAdd(msgTime:Long,srcID:Long,dstID:Long) extends RaphWriteClass
case class EdgeAddWithProperties(msgTime:Long, override val srcID:Long,dstID:Long, properties: Map[String,String]) extends RaphWriteClass
case class EdgeUpdateProperties(msgTime:Long,override val srcID:Long,dstID:Long,property:Map[String,String]) extends RaphWriteClass
case class EdgeRemoval(msgTime:Long,override val srcID:Long,dstID:Long) extends RaphWriteClass

case class EdgeUpdateProperty(msgTime : Long, edgeId : Long, key : String, value : String) //for data coming from the LAM
case class RemoteEdgeUpdateProperties(msgTime:Long,srcID:Long,dstID:Long,properties:Map[String,String])
case class RemoteEdgeAdd(msgTime:Long, srcID:Long, dstID:Long, properties: Map[String,String])
case class RemoteEdgeRemoval(msgTime:Long,srcID:Long,dstID:Long)

case class RemoteEdgeUpdatePropertiesNew(msgTime:Long,srcID:Long,dstID:Long,properties:Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeAddNew(msgTime:Long,srcID:Long,dstID:Long,properties: Map[String,String],kills:mutable.TreeMap[Long, Boolean])
case class RemoteEdgeRemovalNew(msgTime:Long,srcID:Long,dstID:Long,kills:mutable.TreeMap[Long, Boolean])

case class RemoteReturnDeaths(msgTime:Long,srcID:Long,dstID:Long,kills:mutable.TreeMap[Long, Boolean])
case class ReturnEdgeRemoval(msgTime:Long,srcID:Long,dstID:Long)

//BLOCK FROM WORKER SYNC
case class DstAddForOtherWorker(msgTime:Long,dstID:Long,srcForEdge:Long,edge:Edge,present:Boolean)
case class DstWipeForOtherWorker(msgTime:Long,dstID:Long,srcForEdge:Long,edge:Edge,present:Boolean)
case class DstResponseFromOtherWorker(msgTime:Long,srcForEdge:Long,dstID:Long,removeList:mutable.TreeMap[Long, Boolean])
case class EdgeRemoveForOtherWorker(msgTime:Long,srcID:Long,dstID:Long)
case class EdgeRemovalAfterArchiving(msgTime:Long,srcID:Long,dstID:Long)

case class UpdatedCounter(newValue : Int)
case class AssignedId(id : Int)
case class PartitionsCount(count : Int)
case class PartitionsCountResponse(count:Int)
case class RequestPartitionCount()
case class RequestPartitionId()
case class RequestRouterId()

case class CompressVertices(lastSaved:Long,workerID:Int)
case class CompressVertex(key:Long,time:Long)
case class FinishedVertexCompression(key:Long)

case class ArchiveVertices(compressTime:Long,archiveTime:Long,workerID:Int)
case class ArchiveVertex(key:Long,compressTime:Long,archiveTime:Long)
case class ArchiveOnlyVertex(key:Long,archiveTime:Long)
case class FinishedVertexArchiving(key:Long)


case class SetupSlave(children:Int)

case class ReportIntake(mainMessages:Int,secondaryMessages:Int,workerMessages:Int,partitionId:Int,timeDifference:Long)
case class ReportSize(partitionID:Int)

sealed trait RaphReadClasses

abstract class VertexMessage extends java.io.Serializable

case class MessageHandler(vertexID:Int,jobID:String,superStep:Int,message: VertexMessage)

case class Setup(analyzer : Analyser,jobID:String,superStep:Int, timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) extends RaphReadClasses
case class Ready(messages:Int) extends RaphReadClasses
case class NextStep(analyzer : Analyser,jobID:String,superStep:Int, timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) extends RaphReadClasses
case class NextStepNewAnalyser(name: String,jobID:String,superStep:Int, timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) extends RaphReadClasses
case class EndStep(results : Any,messages:Int,voteToHalt:Boolean) extends RaphReadClasses // TODO Define results
case class ExceptionInAnalysis(e:String) extends RaphReadClasses

case class MessagesReceived(workerID:Int,real:Int,receivedMessages:Int,sentMessages:Int) extends RaphReadClasses
case class CheckMessages(superstep:Int) extends RaphReadClasses

case class ReaderWorkersOnline() extends RaphReadClasses
case class ReaderWorkersACK() extends RaphReadClasses

case class AnalyserPresentCheck(classname:String) extends  RaphReadClasses
case class AnalyserPresent() extends  RaphReadClasses
case class ClassMissing() extends RaphReadClasses
case class FailedToCompile (stackTrace:String) extends  RaphReadClasses
case class CompileNewAnalyser(analyser: String, name:String) extends RaphReadClasses
case class ClassCompiled() extends RaphReadClasses
case class TimeCheck(timestamp:Long) extends RaphReadClasses
case class TimeResponse(ok:Boolean,time:Long) extends RaphReadClasses

case class AllocateJob(record:Any)

case class CheckVertex()// extends CheckingFunction
case class CheckEdges()// extends CheckingFunction

case class EdgeAvgTrait()
case class VertexAvgTrait()

case class EdgeAvg()
case class VertexAvg()
//sealed trait CheckingFunction
//case class WatchDogIp(ip: String)

