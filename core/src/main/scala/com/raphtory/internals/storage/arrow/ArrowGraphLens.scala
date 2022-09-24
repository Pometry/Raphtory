package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.entities.ArrowExVertex

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.View

final case class ArrowGraphLens(
                                 jobId: String,
                                 start: Long,
                                 end: Long,
                                 superStep0: Int,
                                 private val par: ArrowPartition,
                                 private val messageSender: GenericVertexMessage[_] => Unit,
                                 private val errorHandler: Throwable => Unit,
                                 scheduler: Scheduler
                               ) extends AbstractGraphLens(
  jobId,
  start,
  end,
  new AtomicInteger(superStep0),
  par,
  messageSender,
  errorHandler,
  scheduler
) {

  override def localNodeCount: Int = {
    val size = par.vertexCount
    assert(size >= 0) // this kind of view knows the size
    size
  }

  override def executeSelect(f: Function2[_, GraphState, Row], graphState: GraphState)(onComplete: => Unit): Unit = ???

  override def executeSelect(f: GraphState => Row, graphState: GraphState)(onComplete: => Unit): Unit = ???

  override def explodeSelect(f: Function[_, IterableOnce[Row]])(onComplete: => Unit): Unit = ???

  override def filteredTable(f: Row => Boolean)(onComplete: => Unit): Unit = ???

  override def explodeTable(f: Row => IterableOnce[Row])(onComplete: => Unit): Unit = ???

  override def explodeView(interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]])(onComplete: => Unit): Unit =
    ???

  override def viewUndirected()(onComplete: => Unit): Unit = ???

  override def viewDirected()(onComplete: => Unit): Unit = ???

  override def viewReversed()(onComplete: => Unit): Unit = ???

  override def reduceView(
                           defaultMergeStrategy: Option[PropertyMerge[_, _]],
                           mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
                           aggregate: Boolean
                         )(onComplete: => Unit): Unit = ???

  //  override def runGraphFunction(f: (_, GraphState) => Unit, graphState: GraphState)(onComplete: => Unit): Unit =
  //    ???

  override def runMessagedGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit = ???

  override def runMessagedGraphFunction(f: (_, GraphState) => Unit, graphState: GraphState)(
    onComplete: => Unit
  ): Unit = ???

  /**
    * Give me the vertices alive at this point
    * use the [[GraphState.isAlive]] to check
    * in the arrow case we'll be passing the local vertex id
    * these also must take into account the [[start]] and [[end]] limits
    *
    * @return
    */
  override def vertices: View[Vertex] = {
    par.windowVertices(start, end)
      .filter(v => graphState.isAlive(v.getLocalId, partitionID()))
      .map(new ArrowExVertex(graphState, _))
  }


}
