package com.raphtory.internals.storage.pojograph

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.RingBuffer
import com.lmax.disruptor.YieldingWaitStrategy
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import com.lmax.disruptor.util.DaemonThreadFactory
import com.raphtory.api.input._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.internals.storage.pojograph.entities.internal.SplitEdge
import com.typesafe.config.Config
import scala.collection.concurrent
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._

private[raphtory] class PojoBasedPartition(graphID: String, partitionID: Int, conf: Config)
        extends GraphPartition(graphID, partitionID, conf) {
  private val hasDeletionsPath      = "raphtory.data.containsDeletions"
  private val hasDeletions: Boolean = conf.getBoolean(hasDeletionsPath)

  logger.debug(
          s"Config indicates that the data contains 'delete' events. " +
            s"To change this modify '$hasDeletionsPath' in the application conf."
  )

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

  sealed trait EdgeToCreate
  final private case object LocalEdge extends EdgeToCreate
  final private case class LocalOutgoingEdge(edge: PojoEdge) extends EdgeToCreate
  final private case class LocalIncomingEdge(edge: PojoEdge) extends EdgeToCreate
  final private case object RemoteOutgoingEdge               extends EdgeToCreate
  final private case object RemoteIncomingEdge               extends EdgeToCreate

  case class VertexAddEvent(
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

  object VertexAddEvent {

    final val EVENT_FACTORY =
      new EventFactory[VertexAddEvent] {
        override def newInstance(): VertexAddEvent = VertexAddEvent()
      }
  }

  class VertexAddEventHandler extends EventHandler[VertexAddEvent] {

    override def onEvent(event: VertexAddEvent, sequence: Long, endOfBatch: Boolean): Unit =
      if (event.dstId == -1L) addVertex(event) else addEdge(event)

    def addProperties(msgTime: Long, index: Long, entity: PojoEntity, properties: Properties): Unit =
      properties addPropertiesToEntity (msgTime, index, entity)

    def addVertex(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, vertexType, edgeType, edgeToCreate) = event

      val vertices = getVerticesMap(srcId)
      vertices.get(srcId) match { // check if the vertex exists
        case Some(v) => // if it does
          v.synchronized {
            v revive (msgTime, index) //add the history point
            addProperties(msgTime, index, v, properties)
          }
          logger.trace(s"History point added to vertex: $srcId")
          logger.trace(s"Properties added: $properties")
        case None    => // if it does not exist
          val v = new PojoVertex(msgTime, index, srcId, initialValue = true) // create a new vertex
          v.setType(vertexType.map(_.name))
          vertices += ((srcId, v)) // put it in the map
          v.synchronized(addProperties(msgTime, index, v, properties))
          logger.trace(s"Properties added: $properties")
          logger.trace(s"New vertex created $srcId")
      }
    }

    def addEdge(event: VertexAddEvent): Unit =
      event.edgeToCreate.getOrElse(new Exception("Unspecified type of edge to create")) match {
        case LocalEdge            => addLocalEdge(event)
        case LocalIncomingEdge(_) => addLocalIncomingEdge(event)
        case LocalOutgoingEdge(_) => addLocalOutgoingEdge(event)
        case RemoteIncomingEdge   => addIncomingEdge(event)
        case RemoteOutgoingEdge   => addOutgoingEdge(event)
      }

    def addLocalEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, vertexType, edgeType, edgeToCreate) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          srcVertex.synchronized {
            edge revive (msgTime, index) //if the edge was previously created we need to revive it
            addProperties(msgTime, index, edge, properties)
          }
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => //if it does not
          //create the new edge, local or remote
          val newEdge = new PojoEdge(msgTime, index, srcId, dstId, initialValue = true)
          newEdge.setType(edgeType.map(_.name))
          srcVertex.synchronized {
            srcVertex addOutgoingEdge newEdge //add this edge to the vertex
            srcVertex addIncomingEdge newEdge // add it to the dst as would not have been seen
            addProperties(msgTime, index, newEdge, properties)
          }
          logger.trace(s"Added incoming & outgoing edge $newEdge to vertex $srcVertex")
      }
    }

    def addLocalOutgoingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, vertexType, edgeType, edgeToCreate) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)
      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) =>
          srcVertex.synchronized {
            edge revive (msgTime, index) //if the edge was previously created we need to revive it
            addProperties(msgTime, index, edge, properties)
          }
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => //if it does not
          //create the new edge, local or remote
          val newEdge = edgeToCreate.get.asInstanceOf[LocalOutgoingEdge].edge
          newEdge.setType(edgeType.map(_.name))
          srcVertex.synchronized {
            srcVertex.addOutgoingEdge(newEdge) //add this edge to the vertex
            addProperties(msgTime, index, newEdge, properties)
          }
          logger.trace(s"Added edge $newEdge to vertex $srcVertex")
      }
    }

    // FIXME Since there is no way to ensure order of 'addLocalOutgoingEdge' and 'addLocalIncomingEdge'
    //  Wonder if this could be an issue ??
    def addLocalIncomingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(_, _, _, dstId, _, _, _, edgeToCreate) = event

      while (!getVerticesMap(dstId).isDefinedAt(dstId))
        Thread.`yield`()

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.synchronized {
        dstVertex addIncomingEdge edgeToCreate.get.asInstanceOf[LocalIncomingEdge].edge
      }
    }

    def addOutgoingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, vertexType, edgeType, edgeToCreate) = event

      while (!getVerticesMap(srcId).isDefinedAt(srcId))
        Thread.`yield`()

      val srcVertex = getVerticesMap(srcId)(srcId)

      srcVertex.getOutgoingEdge(dstId) match {
        case Some(edge) => //retrieve the edge if it exists
          srcVertex.synchronized {
            edge revive (msgTime, index) //if the edge was previously created we need to revive it
            addProperties(msgTime, index, edge, properties)
          }
          logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        case None       => //if it does not
          val newEdge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          logger.trace(s"Split edge $srcId - $dstId between partitions created")
          newEdge.setType(edgeType.map(_.name))
          srcVertex.synchronized {
            srcVertex.addOutgoingEdge(newEdge) //add this edge to the vertex
            addProperties(msgTime, index, newEdge, properties)
          }
          logger.trace(s"Added edge $newEdge to vertex $srcVertex")
      }
    }

    def addIncomingEdge(event: VertexAddEvent): Unit = {
      val VertexAddEvent(msgTime, index, srcId, dstId, properties, vertexType, edgeType, edgeToCreate) = event

      while (!getVerticesMap(dstId).isDefinedAt(dstId))
        Thread.`yield`()

      val dstVertex = getVerticesMap(dstId)(dstId)
      dstVertex.synchronized {
        dstVertex.getIncomingEdge(srcId) match {
          case Some(edge) =>
            dstVertex.synchronized {
              edge revive (msgTime, index) //revive the edge
              edge.setType(edgeType.map(_.name))
              addProperties(msgTime, index, edge, properties)
            }
            logger.debug(s"Edge $srcId $dstId already existed in partition $partitionID for syncNewEdgeAdd")
          case None       =>
            val e = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
            e.setType(edgeType.map(_.name))
            dstVertex.synchronized {
              dstVertex addIncomingEdge e
              addProperties(msgTime, index, e, properties)
            }
            logger.trace(s"added $e to $dstVertex")
        }
      }
    }

  }

  private def forwardAddVertexReqToHandler(
      msgTime: Long,
      index: Long,
      vId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit = {
    val q          = queues(Math.abs(vId % N_LOAD_THREADS).toInt)
    val sequenceId = q.next()
    val event      = q.get(sequenceId)
    event.initAddVertex(msgTime, index, vId, properties, vertexType)
    q.publish(sequenceId)
  }

  // if the add come with some properties add all passed properties into the entity
  override def addVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    forwardAddVertexReqToHandler(msgTime, index, srcId, properties, vertexType)
  // TODO Return only after vertex is added to vertices map.
  //  This guarantees that upon returning from this method vertex has surely been created
//    while (!getVerticesMap(srcId).isDefinedAt(srcId)) // FIXME This won't work if vertex is being revived
//      Thread.`yield`()

  override def addVertex(vAdd: VertexAdd): Unit = {
    val VertexAdd(sourceID, updateTime, index, srcId, properties, vType) = vAdd
    addVertex(sourceID, updateTime, index, srcId, properties, vType)
  }

  private def forwardAddEdgeReqToHandler(
      msgTime: Long,
      index: Long,
      vId: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type],
      edgeToCreate: EdgeToCreate
  ): Unit = {
    val q          = queues(Math.abs((vId % N_LOAD_THREADS).toInt))
    val sequenceId = q.next()
    val event      = q.get(sequenceId)
    event.initAddEdge(msgTime, index, srcId, dstId, properties, edgeType, edgeToCreate)
    q.publish(sequenceId)
  }

  override def addLocalEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // create or revive the src vertex
    forwardAddVertexReqToHandler(msgTime, index, srcId, Properties(), None)
    logger.trace(s"Src ID: $srcId created and revived")

    // create or revive the dst vertex
    if (srcId != dstId) {
      forwardAddVertexReqToHandler(msgTime, index, dstId, Properties(), None)

      val edge = new PojoEdge(msgTime, index, srcId, dstId, initialValue = true)
      forwardAddEdgeReqToHandler(msgTime, index, dstId, srcId, dstId, properties, edgeType, LocalIncomingEdge(edge))
      forwardAddEdgeReqToHandler(msgTime, index, srcId, srcId, dstId, properties, edgeType, LocalOutgoingEdge(edge))
    }
    else
      forwardAddEdgeReqToHandler(msgTime, index, srcId, srcId, dstId, properties, edgeType, LocalEdge)

    // TODO Return only if vertices and edges are created.
    //  This guarantees that upon returning from this method vertices and edges have surely been created
  }

  override def addLocalEdge(eAdd: EdgeAdd): Unit = {
    val EdgeAdd(sourceID, updateTime, index, srcId, dstId, properties, eType) = eAdd
    addLocalEdge(sourceID, updateTime, index, srcId, dstId, properties, eType)
  }

  override def addOutgoingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // create or revive the src vertex
    forwardAddVertexReqToHandler(msgTime, index, srcId, Properties(), None)
    logger.trace(s"Src ID: $srcId created and revived")

    forwardAddEdgeReqToHandler(msgTime, index, srcId, srcId, dstId, properties, edgeType, RemoteOutgoingEdge)

    // TODO Return only if vertices and edges are created.
    //  This guarantees that upon returning from this method vertices and edges have surely been created
  }

  override def addIncomingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // create or revive the src vertex
    forwardAddVertexReqToHandler(msgTime, index, dstId, Properties(), None)
    logger.trace(s"Dst ID: $srcId created and revived")

    forwardAddEdgeReqToHandler(msgTime, index, dstId, srcId, dstId, properties, edgeType, RemoteIncomingEdge)

    // TODO Return only if vertices and edges are created.
    //  This guarantees that upon returning from this method vertices and edges have surely been created
  }

  override def getVertices(
      lens: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex] = {
    val lenz = lens.asInstanceOf[PojoGraphLens]
    import scala.collection.parallel.CollectionConverters._

    arrOfMapOfVertices.map {
      _.par.collect {
        case (id, vertex) if vertex.aliveBetween(start, end) =>
          (id, vertex.viewBetween(start, end, lenz))
      }.seq
    }
  }
    .reduce(_ ++ _)
}
