package com.raphtory.internals.storage.pojograph

import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import com.lmax.disruptor.util.DaemonThreadFactory
import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.RingBuffer
import com.lmax.disruptor.YieldingWaitStrategy
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.internals.storage.pojograph.entities.internal.SplitEdge
import com.typesafe.scalalogging.Logger
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._
import scala.collection.mutable

private[pojograph] class DisruptorQueue(graphID: String, partitionID: Int) {
  import DisruptorQueue._

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val BATCH_EDGES        = true
  private var batchCount         = 0
  private val BATCH_EDGES_SIZE   = 4096
  private lazy val msgTimes      = Array.ofDim[Long](BATCH_EDGES_SIZE)
  private lazy val indexes       = Array.ofDim[Long](BATCH_EDGES_SIZE)
  private lazy val vIds          = Array.ofDim[Long](BATCH_EDGES_SIZE)
  private lazy val srcIds        = Array.ofDim[Long](BATCH_EDGES_SIZE)
  private lazy val dstIds        = Array.ofDim[Long](BATCH_EDGES_SIZE)
  private lazy val propertiess   = Array.ofDim[Properties](BATCH_EDGES_SIZE)
  private lazy val maybeTypes    = Array.ofDim[Option[Type]](BATCH_EDGES_SIZE)
  private lazy val edgeToCreates = Array.ofDim[EdgeToCreate](BATCH_EDGES_SIZE)

  private val N_LOAD_THREADS = Runtime.getRuntime.availableProcessors
  private val QUEUE_SIZE     = 1024 * 32 * 2

  private val arrOfMapOfVertices = Array.fill(N_LOAD_THREADS)(new Long2ObjectOpenHashMap[PojoVertex]())

  def buildDisruptor(): Disruptor[DisruptorEvent] = {
    val threadFactory = DaemonThreadFactory.INSTANCE
    val waitStrategy  = new YieldingWaitStrategy()

    new Disruptor[DisruptorEvent](
            DisruptorEvent.EVENT_FACTORY,
            QUEUE_SIZE,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
    )
  }

  private val queues     = Array.ofDim[RingBuffer[DisruptorEvent]](N_LOAD_THREADS)
  private val disruptors = Array.ofDim[Disruptor[DisruptorEvent]](N_LOAD_THREADS)

  (0 until N_LOAD_THREADS).foreach { i =>
    val eventHandler = new DisruptorEventHandler
    val disruptor    = buildDisruptor()
    disruptor.handleEventsWith(eventHandler)

    disruptors(i) = disruptor
    queues(i) = disruptor.start()
  }

  private def getIndex(vId: Long): Int = Math.abs(vId % N_LOAD_THREADS).toInt

  private def getVerticesMap(vId: Long): Long2ObjectOpenHashMap[PojoVertex] =
    arrOfMapOfVertices(getIndex(vId))

  private def waitUntilVertexIsAdded(vId: Long): Unit =
    while (!getVerticesMap(vId).containsKey(vId))
      Thread.`yield`()

  class DisruptorEventHandler extends EventHandler[DisruptorEvent] {

    override def onEvent(event: DisruptorEvent, sequence: Long, endOfBatch: Boolean): Unit =
      // If dstId is set to -1L, event is considered as an add edge event
      if (event.dstId == -1L) addVertex(event) else addEdge(event)

    private def addProperties(msgTime: Long, index: Long, entity: PojoEntity, properties: Properties): Unit =
      properties addPropertiesToEntity (msgTime, index, entity)

    private def addVertex(event: DisruptorEvent): Unit = {
      val DisruptorEvent(msgTime, index, srcId, _, properties, maybeType, _) = event

      val vertices = getVerticesMap(srcId)
      if (vertices.containsKey(srcId)) { // Check if the vertex exists
        val vertex = vertices.get(srcId)
        vertex revive (msgTime, index) // Add the history point
        addProperties(msgTime, index, vertex, properties)
        logger.trace(s"History point added to vertex: $srcId")
        logger.trace(s"Properties added: $properties")
      }
      else { // If it does not exist
        val vertex = new PojoVertex(msgTime, index, srcId, initialValue = true) // create a new vertex
        vertex.setType(maybeType.map(_.name))
        vertices.put(srcId, vertex) // Put it in the map
        addProperties(msgTime, index, vertex, properties)
        logger.trace(s"Properties added: $properties")
        logger.trace(s"New vertex created $srcId")
      }
    }

    private def addEdge(event: DisruptorEvent): Unit =
      event.edgeToCreate.getOrElse(
              new Exception(s"Unspecified type of edge to create. EdgeToCreate = ${event.edgeToCreate}")
      ) match {
        case SelfLoop             => addLocalEdge(event)
        case LocalIncomingEdge(_) => addLocalIncomingEdge(event)
        case LocalOutgoingEdge(_) => addLocalOutgoingEdge(event)
        case RemoteIncomingEdge   => addIncomingEdge(event)
        case RemoteOutgoingEdge   => addOutgoingEdge(event)
      }

    private def addLocalEdge(event: DisruptorEvent): Unit = {
      val DisruptorEvent(msgTime, index, srcId, dstId, properties, maybeType, _) = event

      waitUntilVertexIsAdded(srcId)

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          // Create the new edge, local or remote
          val edge = new PojoEdge(msgTime, index, srcId, dstId, initialValue = true)
          edge.setType(maybeType.map(_.name))
          srcVertex addOutgoingEdge edge // Add this edge to the vertex
          srcVertex addIncomingEdge edge // Add it to the dst as would not have been seen
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Added incoming & outgoing edge $edge to vertex $srcVertex")
      }
    }

    private def addLocalOutgoingEdge(event: DisruptorEvent): Unit = {
      val DisruptorEvent(msgTime, index, srcId, dstId, properties, _, edgeToCreate) = event

      waitUntilVertexIsAdded(srcId)

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          val edge = edgeToCreate.get.asInstanceOf[LocalOutgoingEdge].edge
          srcVertex addOutgoingEdge edge // Add this edge to the vertex
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Added edge $edge to vertex $srcVertex")
      }
    }

    private def addLocalIncomingEdge(event: DisruptorEvent): Unit = {
      val DisruptorEvent(_, _, srcId, dstId, _, _, edgeToCreate) = event

      waitUntilVertexIsAdded(dstId)

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.getIncomingEdge(srcId) match {
        case Some(_) =>
        case None    =>
          val edge = edgeToCreate.get.asInstanceOf[LocalIncomingEdge].edge
          dstVertex addIncomingEdge edge
          logger.trace(s"Added edge $edge to vertex $dstVertex")
      }
    }

    private def addOutgoingEdge(event: DisruptorEvent): Unit = {
      val DisruptorEvent(msgTime, index, srcId, dstId, properties, maybeType, _) = event

      waitUntilVertexIsAdded(srcId)

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) => // Retrieve the edge if it exists
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          val edge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          logger.trace(s"Split edge $srcId - $dstId between partitions created")
          edge.setType(maybeType.map(_.name))
          srcVertex addOutgoingEdge edge // Add this edge to the vertex
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Added edge $edge to vertex $srcVertex")
      }
    }

    private def addIncomingEdge(event: DisruptorEvent): Unit = {
      val DisruptorEvent(msgTime, index, srcId, dstId, properties, maybeType, _) = event

      waitUntilVertexIsAdded(dstId)

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.getIncomingEdge(srcId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // Revive the edge
          addProperties(msgTime, index, edge, properties)
          logger.debug(s"Edge $srcId $dstId already existed in partition $partitionID for syncNewEdgeAdd")
        case None       =>
          val edge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          edge.setType(maybeType.map(_.name))
          dstVertex addIncomingEdge edge
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"added $edge to $dstVertex")
      }
    }
  }

  def addAddVertexReqToQueue(
      msgTime: Long,
      index: Long,
      vId: Long,
      properties: Properties,
      maybeType: Option[Type]
  ): Unit =
    synchronized {
      val q          = queues(getIndex(vId))
      val sequenceId = q.next()
      val event      = q.get(sequenceId)
      event.initAddVertex(msgTime, index, vId, properties, maybeType)
      q.publish(sequenceId)
    }

  private def pushEdgeToQueue(
      msgTime: Long,
      index: Long,
      vId: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      maybeType: Option[Type],
      edgeToCreate: EdgeToCreate
  ): Unit = {
    val q          = queues(getIndex(vId))
    val sequenceId = q.next()
    val event      = q.get(sequenceId)
    event.initAddEdge(msgTime, index, srcId, dstId, properties, maybeType, edgeToCreate)
    q.publish(sequenceId)
  }

  private def flushBatch(): Unit = {
    for (i <- 0 until batchCount)
      pushEdgeToQueue(
              msgTimes(i),
              indexes(i),
              vIds(i),
              srcIds(i),
              dstIds(i),
              propertiess(i),
              maybeTypes(i),
              edgeToCreates(i)
      )
    batchCount = 0
  }

  def addAddEdgeReqToQueue(
      msgTime: Long,
      index: Long,
      vId: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      maybeType: Option[Type],
      edgeToCreate: EdgeToCreate
  ): Unit =
    synchronized {
      // The idea behind batching only edges is to allow vertices to be created first. The assumption is that
      // since creation of edges waits for vertices to have already been created (see `waitUntilVertexIsAdded`),
      // allowing vertices to be created first should reduce wait time.
      if (BATCH_EDGES) {
        msgTimes(batchCount) = msgTime
        indexes(batchCount) = index
        vIds(batchCount) = vId
        srcIds(batchCount) = srcId
        dstIds(batchCount) = dstId
        propertiess(batchCount) = properties
        maybeTypes(batchCount) = maybeType
        edgeToCreates(batchCount) = edgeToCreate
        batchCount += 1
        if (batchCount == BATCH_EDGES_SIZE)
          flushBatch()
      }
      else pushEdgeToQueue(msgTime, index, vId, srcId, dstId, properties, maybeType, edgeToCreate)
    }

  def getVertices(
      start: Long,
      end: Long,
      f: (Long, PojoVertex) => (Long, PojoExVertex)
  ): mutable.Map[Long, PojoExVertex] =
    arrOfMapOfVertices
      .map {
        _.asScala.collect {
          case (id, vertex) if vertex.aliveBetween(start, end) => f(id, vertex)
        }
      }
      .reduce(_ ++ _)

  def flush(): Unit = synchronized {
    if (BATCH_EDGES && batchCount != 0)
      flushBatch()

    var finished = false
    while (!finished) {
      finished = true

      (0 until N_LOAD_THREADS).foreach { i =>
        if (queues(i).remainingCapacity() != QUEUE_SIZE)
          finished = false
      }

      if (!finished) Thread.sleep(10L)
    }

    // We don't need to halt disruptors here since we may expect graph updates from clients via QuerySender
    // (0 until N_LOAD_THREADS).foreach(i => disruptors(i).halt())

    logger.trace(s"Finished ingesting data for graphID = $graphID, partitionID = $partitionID")
  }

  def stop(): Unit =
    (0 until N_LOAD_THREADS).foreach(i => disruptors(i).halt())
}

object DisruptorQueue {
  sealed trait EdgeToCreate

  final case object SelfLoop extends EdgeToCreate

  final case class LocalOutgoingEdge(edge: PojoEdge) extends EdgeToCreate

  final case class LocalIncomingEdge(edge: PojoEdge) extends EdgeToCreate

  final case object RemoteOutgoingEdge extends EdgeToCreate

  final case object RemoteIncomingEdge extends EdgeToCreate

  final private[DisruptorQueue] case class DisruptorEvent(
      private[pojograph] var msgTime: Long = -1L,
      private[pojograph] var index: Long = -1L,
      private[pojograph] var srcId: Long = -1L,
      private[pojograph] var dstId: Long = -1L,
      private[pojograph] var properties: Properties = Properties(),
      private[pojograph] var maybeType: Option[Type] = None,
      private[pojograph] var edgeToCreate: Option[EdgeToCreate] = None
  ) {

    def initAddVertex(
        msgTime: Long,
        index: Long,
        srcId: Long,
        properties: Properties,
        maybeType: Option[Type]
    ): Unit = {
      this.msgTime = msgTime
      this.index = index
      this.srcId = srcId
      this.dstId = -1L
      this.properties = properties
      this.maybeType = maybeType
      this.edgeToCreate = None
    }

    def initAddEdge(
        msgTime: Long,
        index: Long,
        srcId: Long,
        dstId: Long,
        properties: Properties,
        maybeType: Option[Type],
        edgeToCreate: EdgeToCreate
    ): Unit = {
      this.msgTime = msgTime
      this.index = index
      this.srcId = srcId
      this.dstId = dstId
      this.properties = properties
      this.maybeType = maybeType
      this.edgeToCreate = Some(edgeToCreate)
    }

  }

  private[DisruptorQueue] object DisruptorEvent {

    final val EVENT_FACTORY =
      new EventFactory[DisruptorEvent] {
        override def newInstance(): DisruptorEvent = DisruptorEvent()
      }
  }

  def apply(graphID: String, partitionID: Int): DisruptorQueue = new DisruptorQueue(graphID, partitionID)
}
