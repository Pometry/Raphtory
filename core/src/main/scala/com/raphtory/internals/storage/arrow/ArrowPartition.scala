package com.raphtory.internals.storage.arrow

import com.lmax.disruptor.BlockingWaitStrategy
import com.lmax.disruptor.BusySpinWaitStrategy
import com.lmax.disruptor.SleepingWaitStrategy
import com.lmax.disruptor.YieldingWaitStrategy
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import com.lmax.disruptor.util.DaemonThreadFactory
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation._
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.communication.SchemaProviderInstances.genericSchemaProvider
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config
import net.openhft.hashing.LongHashFunction

import java.lang
import scala.collection.AbstractView
import scala.collection.View
import scala.collection.mutable

class ArrowPartition(graphID: String, val par: RaphtoryArrowPartition, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf)
        with AutoCloseable {

  def asGlobal(vertexId: Long): Long =
    par.getVertexMgr.getVertex(vertexId).getGlobalId

  def getVertex(id: Long): Vertex =
    par.getVertexMgr.getVertex(id)

  def vertexCount: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt

  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  private val nWorkers               = 8
  private val queueSize              = 8192 * 16
  private val workers: Array[Worker] = Array.tabulate(nWorkers)(i => new Worker(i, par, conf))

  private val disruptors: Array[Disruptor[QueuePayload]] = Array.tabulate(nWorkers) { i =>
    val d = buildDisruptor
    d.handleEventsWith(workers(i))
    d
  }

  private val queues = Array.tabulate(nWorkers)(i => disruptors(i).start)

  override def flush(): Unit = {
    var finished = false
    while (!finished) {
      finished = true
      for (i <- 0 until nWorkers)
        if (queues(i).remainingCapacity() != queueSize)
          finished = false

      if (!finished)
        Thread.sleep(10)
    }
  }

  private def buildDisruptor: Disruptor[QueuePayload] = {
    val threadFactory = DaemonThreadFactory.INSTANCE
    //val waitStrategy  = new SleepingWaitStrategy(100, 100)
    val waitStrategy  = new BusySpinWaitStrategy
    //val waitStrategy = new YieldingWaitStrategy
    //val waitStrategy = new BlockingWaitStrategy

    new Disruptor[QueuePayload](
            () => QueuePayload(null, EdgeDirection.NaN),
            queueSize,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
    )
  }

  def localEntityStore: LocalEntityIdStore = par.getLocalEntityIdStore

  def vertices: View[Vertex] =
    new AbstractView[Vertex] {
      override def iterator: Iterator[Vertex] = new ArrowPartition.VertexIterator(par.getNewAllVerticesIterator)

      // can we have more than 2 billion vertices per partition?
      override def knownSize: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt
    }

  def windowVertices(start: Long, end: Long): View[Vertex] =
    View.fromIteratorProvider(() => new ArrowPartition.VertexIterator(par.getNewWindowedVertexIterator(start, end)))

  private def vmgr: VertexPartitionManager = par.getVertexMgr
  private def emgr: EdgePartitionManager   = par.getEdgeMgr

  override def addVertex(
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    addVertex(GraphAlteration.VertexAdd(msgTime, index, srcId, properties, vertexType))

  // we scramble the vertexId to make sure the partitioning algo doesn't send the update to the same file on a partition
  // as long as the vertices are sharded per partition using mod and we hash them with a different function before
  // sending them to a worker they should be well distributed
  private def scramble(id: Long): Long =
    LongHashFunction.xx3().hashLong(id)

  private def addVertex(vAdd: GraphAlteration.VertexAdd): Unit = {
    val worker = findWorker(vAdd.srcId)
    enqueueVertex(vAdd, worker)
  }

  private def enqueueVertex(vAdd: GraphAlteration.VertexAdd, worker: Int): Unit = {
    val sequenceId          = queues(worker).next
    val event: QueuePayload = queues(worker).get(sequenceId)
    event.graphUpdate = vAdd
    event.direction = EdgeDirection.NaN
    queues(worker).publish(sequenceId)
  }

  def addLocalEdge(eAdd: GraphAlteration.EdgeAdd): Unit = {
    val srcWorker           = findWorker(eAdd.srcId)
    val sequenceId          = queues(srcWorker).next()
    val event: QueuePayload = queues(srcWorker).get(sequenceId)
    event.graphUpdate = eAdd
    event.direction = EdgeDirection.NaN
    queues(srcWorker).publish(sequenceId)
  }

  private def findWorker(vertexId: Long): Int =
    Math.abs((scramble(vertexId) % nWorkers).toInt)

  private def addOutgoingEdge(eAdd: GraphAlteration.EdgeAdd): Unit = {
    val srcWorker = findWorker(eAdd.srcId)

    val sequenceId          = queues(srcWorker).next()
    val event: QueuePayload = queues(srcWorker).get(sequenceId)
    event.graphUpdate = eAdd
    event.direction = EdgeDirection.Outgoing
    queues(srcWorker).publish(sequenceId)
  }

  private def addIncomingEdge(eAdd: GraphAlteration.EdgeAdd): Unit = {
    val dstWorker           = findWorker(eAdd.dstId)
    val sequenceId          = queues(dstWorker).next()
    val event: QueuePayload = queues(dstWorker).get(sequenceId)
    event.graphUpdate = eAdd
    event.direction = EdgeDirection.Incoming
    queues(dstWorker).publish(sequenceId)
  }

  // This method should assume that both vertices are local and create them if they don't exist
  override def addLocalEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit =
    addLocalEdge(GraphAlteration.EdgeAdd(msgTime, index, srcId, dstId, properties, edgeType))

  // This method should assume that the dstId belongs to another partition
  override def addOutgoingEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit =
    addOutgoingEdge(GraphAlteration.EdgeAdd(msgTime, index, srcId, dstId, properties, edgeType))

  override def addIncomingEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {

    addIncomingEdge(GraphAlteration.EdgeAdd(msgTime, index, srcId, dstId, properties, edgeType))
  }

  override def getVertices(graphPerspective: LensInterface, start: Long, end: Long): mutable.Map[Long, PojoExVertex] =
    ???

  override def close(): Unit = par.close()
}

object ArrowPartition {

  class PropertyIterator[P](iter: ArrowPropertyIterator)(implicit P: Prop[P]) extends Iterator[Option[(P, Long)]] {
    override def hasNext: Boolean = iter.hasNext

    override def next(): Option[(P, Long)] = {
      val acc = iter.next()
      P.get(acc).map(p => p -> acc.getCreationTime)
    }
  }

  class VertexIterator(vs: com.raphtory.arrowcore.implementation.VertexIterator) extends Iterator[Vertex] {

    override def hasNext: Boolean =
      vs.hasNext

    override def next(): Vertex = {
      vs.next()
      vs.getVertex
    }
  }

  class EdgesIterator(es: EdgeIterator) extends Iterator[Edge] {

    override def hasNext: Boolean =
      es.hasNext

    override def next(): Edge = {
      es.next()
      es.getEdge
    }

  }

  class VertexHistoryIterator(vhi: VertexHistoryIterator.WindowedVertexHistoryIterator)
          extends Iterator[HistoricEvent] {
    override def hasNext: Boolean = vhi.hasNext

    override def next(): HistoricEvent = {
      vhi.next()
      HistoricEvent(vhi.getModificationTime, vhi.getModificationTime, vhi.wasActive())
    }

  }

  class EdgeHistoryIterator(vhi: EdgeHistoryIterator.WindowedEdgeHistoryIterator) extends Iterator[HistoricEvent] {
    override def hasNext: Boolean = vhi.hasNext

    override def next(): HistoricEvent = {
      vhi.next()
      HistoricEvent(vhi.getModificationTime, vhi.getModificationTime, vhi.wasActive())
    }

  }

  class MatchingEdgesIterator(mei: EdgeIterator) extends Iterator[Edge] {
    override def hasNext: Boolean = mei.hasNext

    override def next(): Edge = {
      mei.next()
      mei.getEdge
    }
  }

  def apply(graphId: String, cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(graphId, arrowConfig, arrowConfig.getRaphtoryPartitionId, config)
  }
}
