package com.raphtory.core.components.querymanager

import com.raphtory.core.algorithm.Alignment
import com.raphtory.core.algorithm.GraphFunction
import com.raphtory.core.algorithm.GraphStateImplementation
import com.raphtory.core.algorithm.TableFunction
import com.raphtory.core.time.Interval
import com.raphtory.core.time.NullInterval

import scala.collection.immutable.Queue

/** @DoNotDocument */
trait QueryManagement extends Serializable

case class WatermarkTime(partitionID: Int, startTime: Long, endTime: Long, safe: Boolean)
        extends QueryManagement

case object StartAnalysis                   extends QueryManagement
case class EstablishExecutor(jobID: String) extends QueryManagement
case class ExecutorEstablished(worker: Int) extends QueryManagement

case class CreatePerspective(timestamp: Long, window: Option[Interval]) extends QueryManagement
case class PerspectiveEstablished(vertices: Int)                        extends QueryManagement
case class SetMetaData(vertices: Int)                                   extends QueryManagement
case object MetaDataSet                                                 extends QueryManagement
case object JobDone                                                     extends QueryManagement

case object StartGraph extends QueryManagement

case class GraphFunctionComplete(
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false
) extends QueryManagement

case class GraphFunctionCompleteWithState(
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false,
    graphState: GraphStateImplementation
) extends QueryManagement

case object TableBuilt            extends QueryManagement
case object TableFunctionComplete extends QueryManagement

case object RecheckTime                 extends QueryManagement
case object RecheckEarliestTime         extends QueryManagement
case class CheckMessages(jobId: String) extends QueryManagement

case class VertexMessage[+T](superstep: Int, vertexId: Long, data: T) extends QueryManagement
case class VertexMessageBatch[T](data: Array[VertexMessage[T]])       extends QueryManagement

case class Query(
    name: String = "",
    points: PointSet = NullPointSet,
    timelineStart: Long = Long.MinValue,
    timelineEnd: Long = Long.MaxValue,
    windows: List[Interval] = List(),
    windowAlignment: Alignment.Value = Alignment.END,
    graphFunctions: Queue[GraphFunction] = Queue(),
    tableFunctions: Queue[TableFunction] = Queue()
) extends QueryManagement

sealed trait PointSet
case object NullPointSet           extends PointSet
case class SinglePoint(time: Long) extends PointSet

case class PointPath(
    increment: Interval,
    start: Long = Long.MinValue,
    end: Long = Long.MaxValue,
    offset: Interval = NullInterval,
    customStart: Boolean = false
) extends PointSet

case class EndQuery(jobID: String)        extends QueryManagement
case class QueryNotPresent(jobID: String) extends QueryManagement

case class TaskFinished(result: Boolean) extends QueryManagement
case object AreYouFinished               extends QueryManagement
