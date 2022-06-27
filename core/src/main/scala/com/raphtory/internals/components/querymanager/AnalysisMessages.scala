package com.raphtory.internals.components.querymanager

import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.GraphFunction
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.time.Interval
import com.raphtory.api.time.NullInterval
import com.raphtory.internals.graph.Perspective

import scala.collection.immutable.Queue

private[raphtory] trait QueryManagement extends Serializable

private[raphtory] case class WatermarkTime(
    partitionID: Int,
    oldestTime: Long,
    latestTime: Long,
    safe: Boolean
) extends QueryManagement

private[raphtory] case object StartAnalysis                               extends QueryManagement
private[raphtory] case class EstablishExecutor(jobID: String, sink: Sink) extends QueryManagement

private[raphtory] case class SetMetaData(vertices: Int) extends QueryManagement

private[raphtory] case object JobDone extends QueryManagement

private[raphtory] case class CreatePerspective(id: Int, perspective: Perspective) extends QueryManagement

private[raphtory] case object StartGraph extends QueryManagement

private[raphtory] case object CompleteWrite extends QueryManagement

private[raphtory] case object RecheckTime                 extends QueryManagement
private[raphtory] case object RecheckEarliestTime         extends QueryManagement
private[raphtory] case class CheckMessages(jobId: String) extends QueryManagement

sealed private[raphtory] trait GenericVertexMessage[VertexID] extends QueryManagement {
  def superstep: Int
  def vertexId: VertexID
}

private[raphtory] case class VertexMessage[+T, VertexID](
    superstep: Int,
    vertexId: VertexID,
    data: T
) extends GenericVertexMessage[VertexID]

private[raphtory] case class VertexMessageBatch(data: Array[GenericVertexMessage[_]]) extends QueryManagement

private[raphtory] case class FilteredEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
) extends GenericVertexMessage[VertexID]

private[raphtory] case class FilteredInEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
) extends GenericVertexMessage[VertexID]

private[raphtory] case class FilteredOutEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
) extends GenericVertexMessage[VertexID]

private[raphtory] case class Query(
    name: String = "",
    points: PointSet = NullPointSet,
    timelineStart: Long = Long.MinValue, // inclusive
    timelineEnd: Long = Long.MaxValue,   // inclusive
    windows: List[Interval] = List(),
    windowAlignment: Alignment.Value = Alignment.START,
    graphFunctions: Queue[GraphFunction] = Queue(),
    tableFunctions: Queue[TableFunction] = Queue(),
    sink: Option[Sink] = None
) extends QueryManagement

sealed private[raphtory] trait PointSet
private[raphtory] case object NullPointSet           extends PointSet
private[raphtory] case class SinglePoint(time: Long) extends PointSet

private[raphtory] case class PointPath(
    increment: Interval,
    start: Option[Long] = None,
    end: Option[Long] = None,
    offset: Interval = NullInterval
) extends PointSet

private[raphtory] case class EndQuery(jobID: String)        extends QueryManagement
private[raphtory] case class QueryNotPresent(jobID: String) extends QueryManagement

// Messages for jobStatus topic
sealed private[raphtory] trait JobStatus extends QueryManagement

private[raphtory] case class ExecutorEstablished(worker: Int) extends JobStatus
private[raphtory] case object WriteCompleted                  extends JobStatus

sealed private[raphtory] trait PerspectiveStatus extends JobStatus {
  def perspectiveID: Int
}

private[raphtory] case class PerspectiveEstablished(perspectiveID: Int, vertices: Int) extends PerspectiveStatus
private[raphtory] case class MetaDataSet(perspectiveID: Int)                           extends PerspectiveStatus

private[raphtory] case class GraphFunctionComplete(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false
) extends PerspectiveStatus

private[raphtory] case class GraphFunctionCompleteWithState(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false,
    graphState: GraphStateImplementation
) extends PerspectiveStatus

private[raphtory] case class TableFunctionComplete(perspectiveID: Int) extends PerspectiveStatus
private[raphtory] case class TableBuilt(perspectiveID: Int)            extends PerspectiveStatus

private[raphtory] case class AlgorithmFailure(perspectiveID: Int, exception: Throwable) extends PerspectiveStatus
