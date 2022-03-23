package com.raphtory.core.components.querymanager

import com.raphtory.core.algorithm.Alignment
import com.raphtory.core.algorithm.GraphFunction
import com.raphtory.core.algorithm.GraphStateImplementation
import com.raphtory.core.algorithm.TableFunction
import com.raphtory.core.time.Interval

import scala.collection.immutable.Queue

/** @DoNotDocument */
trait QueryManagement extends Serializable

case class WatermarkTime(partitionID: Int, time: Long, safe: Boolean) extends QueryManagement

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
case class CheckMessages(jobId: String) extends QueryManagement

case class VertexMessage[+T](superstep: Int, vertexId: Long, data: T) extends QueryManagement
case class VertexMessageBatch[T](data: Array[VertexMessage[T]])       extends QueryManagement

case class Query(
    name: String = "",
    graphFunctions: Queue[GraphFunction] = Queue(),
    tableFunctions: Queue[TableFunction] = Queue(),
    startTime: Option[Long] = None,
    endTime: Option[Long] = None,
    increment: Option[Interval] = None,
    windows: List[Interval] = List()
) extends QueryManagement

case class EndQuery(jobID: String)        extends QueryManagement
case class QueryNotPresent(jobID: String) extends QueryManagement

case class TaskFinished(result: Boolean) extends QueryManagement
case object AreYouFinished               extends QueryManagement
