package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.graphstate.GraphState
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

  /**
    * Give me the vertices alive at this point
    * use the [[GraphState]] to check
    * in the arrow case we'll be passing the local vertex id
    * these also must take into account the [[start]] and [[end]] limits
    *
    * @return
    */
  override def vertices: View[Vertex] =
    par
      .windowVertices(start, end)
      .filter(v => graphState.isAlive(v.getGlobalId))
      .map(new ArrowExVertex(graphState, _))

  override def executeSelect(values: Seq[String], defaults: Map[String, Any])(onComplete: () => Unit): Unit = ???

  override def explodeColumns(columns: Seq[String])(onComplete: () => Unit): Unit = ???

  override def renameColumn(columns: Seq[(String, String)])(onComplete: () => Unit): Unit = ???
}
