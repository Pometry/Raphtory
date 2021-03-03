package com.raphtory.core.model.communication

import com.raphtory.api.Analyser
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Edge

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait Property {
  def key: String
  def value: Any
}

case class Type(name: String)
case class ImmutableProperty(key: String, value: String) extends Property
case class StringProperty(key: String, value: String)    extends Property
case class LongProperty(key: String, value: Long)        extends Property
case class DoubleProperty(key: String, value: Double)    extends Property
case class Properties(property: Property*)

sealed trait GraphUpdate {
  val updateTime: Long
  val srcId: Long
}

case class VertexAdd(updateTime: Long, srcId: Long, properties: Properties, vType: Option[Type]) extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
case class VertexDelete(updateTime: Long, srcId: Long) extends GraphUpdate
case class EdgeAdd(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, eType: Option[Type]) extends GraphUpdate
case class EdgeDelete(updateTime: Long, srcId: Long, dstId: Long) extends GraphUpdate

case class TrackedGraphUpdate[+T <: GraphUpdate](channelId: String, channelTime:Int, update: T)

sealed abstract class GraphUpdateEffect(val updateId: Long) extends Serializable {
  val msgTime: Long
}

case class RemoteEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemovalFromVertex(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(dstId)
case class RemoteEdgeAddNew(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, kills: mutable.TreeMap[Long, Boolean], vType: Option[Type]) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemovalNew(msgTime: Long, srcId: Long, dstId: Long, kills: mutable.TreeMap[Long, Boolean]) extends GraphUpdateEffect(dstId)
case class RemoteReturnDeaths(msgTime: Long, srcId: Long, dstId: Long, kills: mutable.TreeMap[Long, Boolean]) extends GraphUpdateEffect(srcId)
case class ReturnEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(srcId)

//BLOCK FROM WORKER SYNC
case class DstAddForOtherWorker(msgTime: Long, srcId: Long, dstId: Long, edge: Edge, present: Boolean) extends GraphUpdateEffect(dstId)
case class DstWipeForOtherWorker(msgTime: Long, srcId: Long, dstId: Long, edge: Edge, present: Boolean) extends GraphUpdateEffect(dstId)
case class DstResponseFromOtherWorker(msgTime: Long, srcId: Long, dstId: Long, removeList: mutable.TreeMap[Long, Boolean]) extends GraphUpdateEffect(srcId)
case class EdgeRemoveForOtherWorker(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(srcId)
case class EdgeSyncAck(msgTime: Long, srcId: Long) extends GraphUpdateEffect(srcId)
case class VertexRemoveSyncAck(msgTime: Long, override val updateId: Long) extends GraphUpdateEffect(updateId)

case class TrackedGraphEffect[T <: GraphUpdateEffect](channelId: String, channelTime: Int, effect: T)


sealed trait RaphReadClasses

case class VertexMessage(vertexID: Long, viewJob: ViewJob, superStep: Int, data:Any )


case class Setup(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class SetupNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class Ready(messages: Int) extends RaphReadClasses
case class NextStep(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class NextStepNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class EndStep(messages: Int, voteToHalt: Boolean) extends RaphReadClasses
case class Finish(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class FinishNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class ReturnResults(results: Any)
case class ExceptionInAnalysis(e: String) extends RaphReadClasses

case class MessagesReceived(workerID: Int, receivedMessages: Int, sentMessages: Int) extends RaphReadClasses
case class CheckMessages(jobID:ViewJob,superstep: Int) extends RaphReadClasses

case class ReaderWorkersOnline() extends RaphReadClasses
case class ReaderWorkersACK()    extends RaphReadClasses



case class LiveAnalysisPOST(analyserName:String, windowType:Option[String], windowSize:Option[Long], windowSet:Option[Array[Long]],repeatTime:Option[Long],eventTime:Option[Boolean],args:Option[Array[String]],rawFile:Option[String])
case class ViewAnalysisPOST(analyserName:String,timestamp:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]],args:Option[Array[String]],rawFile:Option[String])
case class RangeAnalysisPOST(analyserName:String,start:Long,end:Long,jump:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]],args:Option[Array[String]],rawFile:Option[String])
trait AnalysisRequest
case class LiveAnalysisRequest(
    analyserName: String,
    repeatTime:Long =0L,
    eventTime:Boolean=false,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest
case class ViewAnalysisRequest(
    analyserName: String,
    timestamp: Long,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest
case class RangeAnalysisRequest(
    analyserName: String,
    start: Long,
    end: Long,
    jump: Long,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest

case class AnalyserPresentCheck(className: String)            extends RaphReadClasses
case class AnalyserPresent()                                  extends RaphReadClasses
case class ClassMissing()                                     extends RaphReadClasses
case class FailedToCompile(stackTrace: String)                extends RaphReadClasses
case class CompileNewAnalyser(analyser: String,args:Array[String], name: String) extends RaphReadClasses
case class ClassCompiled()                                    extends RaphReadClasses
case class TimeCheck(timestamp: Long)                         extends RaphReadClasses
case class TimeResponse(ok: Boolean, time: Long)              extends RaphReadClasses
case class RequestResults(jobID:String)
case class KillTask(jobID:String)
case class JobKilled()
case class ResultsForApiPI(results:Array[String])
case class JobDoesntExist()
case class AllocateTuple(record: Any)
