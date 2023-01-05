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
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.collection.mutable

private[pojograph] class DisruptorQueue(graphID: String, partitionID: Int) {
  import DisruptorQueue._

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val N_LOAD_THREADS = 8
  private val QUEUE_SIZE     = 1024 * 32 * 2

  private val arrOfMapOfVertices = Array.fill(N_LOAD_THREADS)(new ConcurrentHashMap[Long, PojoVertex]().asScala)

  def buildDisruptor(): Disruptor[VertexAddEvent] = {
    val threadFactory = DaemonThreadFactory.INSTANCE
    val waitStrategy  = new YieldingWaitStrategy()

    new Disruptor[VertexAddEvent](
            VertexAddEvent.EVENT_FACTORY,
            QUEUE_SIZE,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
    )
  }

  private val eventHandlers = Array.ofDim[VertexAddEventHandler](N_LOAD_THREADS)
  private val queues        = Array.ofDim[RingBuffer[VertexAddEvent]](N_LOAD_THREADS)
  private val disruptors    = Array.ofDim[Disruptor[VertexAddEvent]](N_LOAD_THREADS)

  (0 until N_LOAD_THREADS).foreach { i =>
    val eventHandler = new VertexAddEventHandler
    val disruptor    = buildDisruptor()
    disruptor.handleEventsWith(eventHandler)

    eventHandlers(i) = eventHandler
    disruptors(i) = disruptor
    queues(i) = disruptor.start()
  }

  private def getVerticesMap(vId: Long): concurrent.Map[Long, PojoVertex] =
    arrOfMapOfVertices(Math.abs(vId % N_LOAD_THREADS).toInt)

  class VertexAddEventHandler extends EventHandler[VertexAddEvent] {

    override def onEvent(event: VertexAddEvent, sequence: Long, endOfBatch: Boolean): Unit =
      if (event.dstId == -1L) addVertex(event) else addEdge(event)

    def addProperties(msgTime: Long, index: Long, entity: PojoEntity, properties: Properties): Unit =
      properties addPropertiesToEntity (msgTime, index, entity)

    def addVertex(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, _, properties, vertexType, _, _) = event

      val vertices = getVerticesMap(srcId)
      vertices.get(srcId) match { // Check if the vertex exists
        case Some(v) => // If it does
          v revive (msgTime, index) // Add the history point
          addProperties(msgTime, index, v, properties)
          logger.trace(s"History point added to vertex: $srcId")
          logger.trace(s"Properties added: $properties")

        case None    => // If it does not exist
          val v = new PojoVertex(msgTime, index, srcId, initialValue = true) // create a new vertex
          v.setType(vertexType.map(_.name))
          vertices += ((srcId, v)) // Put it in the map
          addProperties(msgTime, index, v, properties)
          logger.trace(s"Properties added: $properties")
          logger.trace(s"New vertex created $srcId")
      }
    }

    def addEdge(event: VertexAddEvent): Unit =
      event.edgeToCreate.getOrElse(
              new Exception(s"Unspecified type of edge to create. EdgeToCreate = ${event.edgeToCreate}")
      ) match {
        case LocalEdge            => addLocalEdge(event)
        case LocalIncomingEdge(_) => addLocalIncomingEdge(event)
        case LocalOutgoingEdge(_) => addLocalOutgoingEdge(event)
        case RemoteIncomingEdge   => addIncomingEdge(event)
        case RemoteOutgoingEdge   => addOutgoingEdge(event)
      }

    def addLocalEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, _, edgeType, _) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          // Create the new edge, local or remote
          val newEdge = new PojoEdge(msgTime, index, srcId, dstId, initialValue = true)
          newEdge.setType(edgeType.map(_.name))
          srcVertex addOutgoingEdge newEdge // Add this edge to the vertex
          srcVertex addIncomingEdge newEdge // Add it to the dst as would not have been seen
          addProperties(msgTime, index, newEdge, properties)
          logger.trace(s"Added incoming & outgoing edge $newEdge to vertex $srcVertex")
      }
    }

    def addLocalOutgoingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, _, edgeType, edgeToCreate) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          val newEdge = edgeToCreate.get.asInstanceOf[LocalOutgoingEdge].edge
          srcVertex.addOutgoingEdge(newEdge) // Add this edge to the vertex
          addProperties(msgTime, index, newEdge, properties)
          logger.trace(s"Added edge $newEdge to vertex $srcVertex")
      }
    }

    def addLocalIncomingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, _, _, edgeToCreate) = event

      while (!getVerticesMap(dstId).isDefinedAt(dstId))
        Thread.`yield`()

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.getIncomingEdge(srcId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       =>
          val newEdge = edgeToCreate.get.asInstanceOf[LocalIncomingEdge].edge
          dstVertex.addIncomingEdge(newEdge)
          logger.trace(s"Added edge $newEdge to vertex $dstVertex")
      }
    }

    def addOutgoingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, _, edgeType, _) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) => // Retrieve the edge if it exists
          edge revive (msgTime, index) // If the edge was previously created we need to revive it
          addProperties(msgTime, index, edge, properties)
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => // If it does not
          val newEdge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          logger.trace(s"Split edge $srcId - $dstId between partitions created")
          newEdge.setType(edgeType.map(_.name))
          srcVertex.addOutgoingEdge(newEdge) // Add this edge to the vertex
          addProperties(msgTime, index, newEdge, properties)
          logger.trace(s"Added edge $newEdge to vertex $srcVertex")
      }
    }

    def addIncomingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, _, edgeType, _) = event

      while (!getVerticesMap(dstId).isDefinedAt(dstId))
        Thread.`yield`()

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.getIncomingEdge(srcId) match {
        case Some(edge) =>
          edge revive (msgTime, index) // Revive the edge
          edge.setType(edgeType.map(_.name))
          addProperties(msgTime, index, edge, properties)
          logger.debug(s"Edge $srcId $dstId already existed in partition $partitionID for syncNewEdgeAdd")
        case None       =>
          val e = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          e.setType(edgeType.map(_.name))
          dstVertex addIncomingEdge e
          addProperties(msgTime, index, e, properties)
          logger.trace(s"added $e to $dstVertex")
      }
    }

  }

  private def getQueue(vId: Long) = queues(Math.abs(vId % N_LOAD_THREADS).toInt)

  def addAddVertexReqToQueue(
      msgTime: Long,
      index: Long,
      vId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit = {
    val q          = getQueue(vId)
    val sequenceId = q.next()
    val event      = q.get(sequenceId)
    event.initAddVertex(msgTime, index, vId, properties, vertexType)
    q.publish(sequenceId)
  }

  def addAddEdgeReqToQueue(
      msgTime: Long,
      index: Long,
      vId: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type],
      edgeToCreate: EdgeToCreate
  ): Unit = {
    val q          = getQueue(vId)
    val sequenceId = q.next()
    val event      = q.get(sequenceId)
    event.initAddEdge(msgTime, index, srcId, dstId, properties, edgeType, edgeToCreate)
    q.publish(sequenceId)
  }

  def getVertices(
      start: Long,
      end: Long,
      f: (Long, PojoVertex) => (Long, PojoExVertex)
  ): mutable.Map[Long, PojoExVertex] = {
    import scala.collection.parallel.CollectionConverters._

    arrOfMapOfVertices.map {
      _.par.collect {
        case (id, vertex) if vertex.aliveBetween(start, end) => f(id, vertex)
      }.seq
    }
  }
    .reduce(_ ++ _)

  def flush(): Unit = {
    var finished = false
    while (!finished) {
      finished = true

      (0 until N_LOAD_THREADS).foreach { i =>
        if (queues(i).remainingCapacity() != QUEUE_SIZE)
          finished = false
      }

      if (!finished) Thread.sleep(10L)
    }

    (0 until N_LOAD_THREADS).foreach(i => disruptors(i).halt())

    logger.trace(s"Finished ingesting data for graphID = $graphID, partitionID = $partitionID")
  }
}

private[pojograph] object DisruptorQueue {
  sealed trait EdgeToCreate

  final case object LocalEdge extends EdgeToCreate

  final case class LocalOutgoingEdge(edge: PojoEdge) extends EdgeToCreate

  final case class LocalIncomingEdge(edge: PojoEdge) extends EdgeToCreate

  final case object RemoteOutgoingEdge extends EdgeToCreate

  final case object RemoteIncomingEdge extends EdgeToCreate

  final private[DisruptorQueue] case class VertexAddEvent(
      private[pojograph] var msgTime: Long = -1L,
      private[pojograph] var index: Long = -1L,
      private[pojograph] var srcId: Long = -1L,
      private[pojograph] var dstId: Long = -1L,
      private[pojograph] var properties: Properties = Properties(),
      private[pojograph] var vertexType: Option[Type] = None,
      private[pojograph] var edgeType: Option[Type] = None,
      private[pojograph] var edgeToCreate: Option[EdgeToCreate] = None
  ) {

    def initAddVertex(
        msgTime: Long,
        index: Long,
        srcId: Long,
        properties: Properties,
        vertexType: Option[Type]
    ): Unit = {
      this.msgTime = msgTime
      this.index = index
      this.srcId = srcId
      this.dstId = -1L
      this.properties = properties
      this.vertexType = vertexType
      this.edgeType = None
      this.edgeToCreate = None
    }

    def initAddEdge(
        msgTime: Long,
        index: Long,
        srcId: Long,
        dstId: Long,
        properties: Properties,
        edgeType: Option[Type],
        edgeToCreate: EdgeToCreate
    ): Unit = {
      this.msgTime = msgTime
      this.index = index
      this.srcId = srcId
      this.dstId = dstId
      this.properties = properties
      this.vertexType = None
      this.edgeType = edgeType
      this.edgeToCreate = Some(edgeToCreate)
    }

  }

  private[DisruptorQueue] object VertexAddEvent {

    final val EVENT_FACTORY =
      new EventFactory[VertexAddEvent] {
        override def newInstance(): VertexAddEvent = VertexAddEvent()
      }
  }

  def apply(graphID: String, partitionID: Int): DisruptorQueue = new DisruptorQueue(graphID, partitionID)
}
