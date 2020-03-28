package com.raphtory.core.model.communication

import java.util.concurrent.ConcurrentHashMap

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.graphentities.Edge

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait GraphUpdate {
  def srcID: Long
}

trait SpoutGoing

case class RouterUp(id: Int)
case class PartitionUp(id: Int)
case class ClusterStatusRequest()
case class ClusterStatusResponse(clusterUp: Boolean)

sealed trait Property {
  def key: String
  def value: Any
}

case class Type(name: String)
case class ImmutableProperty(override val key: String, override val value: String) extends Property
case class StringProperty(override val key: String, override val value: String)    extends Property
case class LongProperty(override val key: String, override val value: Long)        extends Property
case class DoubleProperty(override val key: String, override val value: Long)      extends Property

case class Properties(property: Property*)

case class VertexAdd(msgTime: Long, override val srcID: Long, vType: Type = null) extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgTime: Long, override val srcID: Long, properties: Properties, vType: Type = null)
        extends GraphUpdate
case class VertexUpdateProperties(msgTime: Long, override val srcID: Long, propery: Properties, vType: Type = null)
        extends GraphUpdate
case class VertexDelete(msgTime: Long, override val srcID: Long) extends GraphUpdate

case class EdgeAdd(msgTime: Long, srcID: Long, dstID: Long, eType: Type = null) extends GraphUpdate
case class EdgeAddWithProperties(
    msgTime: Long,
    override val srcID: Long,
    dstID: Long,
    properties: Properties,
    eType: Type = null
) extends GraphUpdate
case class EdgeUpdateProperties(
    msgTime: Long,
    override val srcID: Long,
    dstID: Long,
    property: Properties,
    eType: Type = null
) extends GraphUpdate
case class EdgeDelete(msgTime: Long, override val srcID: Long, dstID: Long) extends GraphUpdate

case class RemoteEdgeUpdateProperties(msgTime: Long, srcID: Long, dstID: Long, properties: Properties, eType: Type)
case class RemoteEdgeAdd(msgTime: Long, srcID: Long, dstID: Long, properties: Properties, eType: Type)
case class RemoteEdgeRemoval(msgTime: Long, srcID: Long, dstID: Long)

case class RemoteEdgeUpdatePropertiesNew(
    msgTime: Long,
    srcID: Long,
    dstID: Long,
    properties: Properties,
    kills: mutable.TreeMap[Long, Boolean],
    vType: Type
)
case class RemoteEdgeAddNew(
    msgTime: Long,
    srcID: Long,
    dstID: Long,
    properties: Properties,
    kills: mutable.TreeMap[Long, Boolean],
    vType: Type
)
case class RemoteEdgeRemovalNew(msgTime: Long, srcID: Long, dstID: Long, kills: mutable.TreeMap[Long, Boolean])

case class RemoteReturnDeaths(msgTime: Long, srcID: Long, dstID: Long, kills: mutable.TreeMap[Long, Boolean])
case class ReturnEdgeRemoval(msgTime: Long, srcID: Long, dstID: Long)

//BLOCK FROM WORKER SYNC
case class DstAddForOtherWorker(msgTime: Long, dstID: Long, srcForEdge: Long, edge: Edge, present: Boolean)
case class DstWipeForOtherWorker(msgTime: Long, dstID: Long, srcForEdge: Long, edge: Edge, present: Boolean)
case class DstResponseFromOtherWorker(
    msgTime: Long,
    srcForEdge: Long,
    dstID: Long,
    removeList: mutable.TreeMap[Long, Boolean]
)
case class EdgeRemoveForOtherWorker(msgTime: Long, srcID: Long, dstID: Long)
case class EdgeRemovalAfterArchiving(msgTime: Long, srcID: Long, dstID: Long)

case class UpdatedCounter(newValue: Int)
case class AssignedId(id: Int)
case class PartitionsCount(count: Int)
case class PartitionsCountResponse(count: Int)
case class RequestPartitionCount()
case class RequestPartitionId()
case class RequestRouterId()

case class CompressVertices(lastSaved: Long, workerID: Int)
case class CompressVertex(key: Long, time: Long)
case class FinishedVertexCompression(key: Long)

case class ArchiveVertices(compressTime: Long, archiveTime: Long, workerID: Int)
case class ArchiveVertex(key: Long, compressTime: Long, archiveTime: Long)
case class ArchiveOnlyVertex(key: Long, archiveTime: Long)
case class FinishedVertexArchiving(key: Long)

case class SetupSlave(children: Int)

case class ReportIntake(
    mainMessages: Int,
    secondaryMessages: Int,
    workerMessages: Int,
    partitionId: Int,
    timeDifference: Long
)
case class ReportSize(partitionID: Int)

sealed trait RaphReadClasses

sealed trait VertexMessage extends java.io.Serializable
case class VertexMessageString(source: Long, vertexID: Long, jobID: String, superStep: Int, data: String)
        extends VertexMessage
case class VertexMessageLong(source: Long, vertexID: Long, jobID: String, superStep: Int, data: Long)
        extends VertexMessage
case class VertexMessageInt(source: Long, vertexID: Long, jobID: String, superStep: Int, data: Int)
        extends VertexMessage
case class VertexMessageFloat(source: Long, vertexID: Long, jobID: String, superStep: Int, data: Float)
        extends VertexMessage
case class VertexMessageBatch(
    jobID: String,
    superStep: Int,
    data: ConcurrentHashMap.KeySetView[(Long, Long, Any), java.lang.Boolean]
) extends VertexMessage

case class Setup(
    analyzer: Analyser,
    jobID: String,
    superStep: Int,
    timestamp: Long,
    analysisType: AnalysisType.Value,
    window: Long,
    windowSet: Array[Long]
) extends RaphReadClasses
case class Ready(messages: Int) extends RaphReadClasses
case class NextStep(
    analyzer: Analyser,
    jobID: String,
    superStep: Int,
    timestamp: Long,
    analysisType: AnalysisType.Value,
    window: Long,
    windowSet: Array[Long]
) extends RaphReadClasses
case class NextStepNewAnalyser(
    name: String,
    jobID: String,
    superStep: Int,
    timestamp: Long,
    analysisType: AnalysisType.Value,
    window: Long,
    windowSet: Array[Long]
) extends RaphReadClasses
case class EndStep(messages: Int, voteToHalt: Boolean) extends RaphReadClasses
case class Finish(
    analyzer: Analyser,
    jobID: String,
    superStep: Int,
    timestamp: Long,
    analysisType: AnalysisType.Value,
    window: Long,
    windowSet: Array[Long]
) extends RaphReadClasses
case class ReturnResults(results: Any)
case class ExceptionInAnalysis(e: String) extends RaphReadClasses

case class MessagesReceived(workerID: Int, real: Int, receivedMessages: Int, sentMessages: Int) extends RaphReadClasses
case class CheckMessages(superstep: Int)                                                        extends RaphReadClasses

case class ReaderWorkersOnline() extends RaphReadClasses
case class ReaderWorkersACK()    extends RaphReadClasses

case class AnalyserPresentCheck(classname: String)            extends RaphReadClasses
case class AnalyserPresent()                                  extends RaphReadClasses
case class ClassMissing()                                     extends RaphReadClasses
case class FailedToCompile(stackTrace: String)                extends RaphReadClasses
case class CompileNewAnalyser(analyser: String, name: String) extends RaphReadClasses
case class ClassCompiled()                                    extends RaphReadClasses
case class TimeCheck(timestamp: Long)                         extends RaphReadClasses
case class TimeResponse(ok: Boolean, time: Long)              extends RaphReadClasses

case class AllocateJob(record: Any)
