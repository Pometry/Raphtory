package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.arrowcore.implementation.RaphtoryThreadPool
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.entities.ArrowExVertex

import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
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

  override def parAggregate[B](init: B)(mapper: Vertex => B)(acc: (B, B) => B): B = {
    val mtIterator = new VertexIterator.MTWindowedVertexManager
    mtIterator.init(par.par.getVertexMgr, RaphtoryThreadPool.THREAD_POOL, start, end)

    val topB = new AtomicReference[B](init)

    mtIterator.start { (pid, iter) =>
      var localB = init
      val start  = LocalDateTime.now()
      println(s"$pid STARTING PROCESSING AT $start")
      while (iter.hasNext) {
        iter.next()
        val v    = iter.getVertex
        val arrV = new ArrowExVertex(graphState, v)

        val b = mapper(arrV)
        localB = acc(localB, b)
      }

      val end = LocalDateTime.now()
      println(s"$pid ENDING PROCESSING AT $end took ${Duration.between(start, end).toMillis}ms processed $localB items")
      topB.accumulateAndGet(localB, (b1, b2) => acc(b1, b2))
    }

    mtIterator.waitTilComplete()

    topB.get()
  }
}
