package com.raphtory.arrowcore

import com.lmax.disruptor.{EventFactory, EventHandler, RingBuffer, YieldingWaitStrategy}
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import com.lmax.disruptor.util.DaemonThreadFactory
import com.raphtory.arrowcore.implementation._
import com.raphtory.arrowcore.model._

import java.io._
import java.util
import java.util.Date

object AlphaBayMTApp extends App {

  val DATA_DIR  = "/tmp/alphabay/data"
  val ARROW_DIR = "/tmp/alphabay/arrow"

  class AlphaBayLoader(rap: RaphtoryArrowPartition) {
    private val BUFFER_SIZE    = 64 * 1024
    private val N_LOAD_THREADS = 8
    private val QUEUE_SIZE     = 32768 * 2

    private val aepm = rap.getEdgeMgr
    private val avpm = rap.getVertexMgr

    private val nodeIdField: Int                           = rap.getVertexFieldId("globalid")
    private val priceProperty: Int                         = rap.getEdgePropertyId("price")
    private val priceVEPA: VersionedEntityPropertyAccessor = rap.getEdgePropertyAccessor(priceProperty)
    private val leis: LocalEntityIdStore                   = rap.getLocalEntityIdStore

    class VertexAddEvent {
      private[arrowcore] var globalId: Long    = -1L
      private[arrowcore] var dstGlobalId: Long = -1L
      private[arrowcore] var nodeId: Long      = -1L
      private[arrowcore] var time: Long        = -1L
      private[arrowcore] var price: Long       = -1L

      def initAddVertex(globalId: Long, nodeId: Long, time: Long): Unit = {
        this.globalId = globalId
        this.dstGlobalId = -1L
        this.nodeId = nodeId
        this.time = time
        this.price = 0L
      }

      def initAddEdge(srcGlobalId: Long, dstGlobalId: Long, time: Long, price: Long): Unit = {
        this.globalId = srcGlobalId
        this.dstGlobalId = dstGlobalId
        this.nodeId = -1L
        this.time = time
        this.price = price
      }
    }

    object VertexAddEvent {
      final val EVENT_FACTORY =
            new EventFactory[VertexAddEvent] {
              override def newInstance(): VertexAddEvent = new VertexAddEvent()
            }
    }

    //noinspection AccessorLikeMethodIsUnit
    class Worker(id: Int) extends EventHandler[VertexAddEvent] {
      private var lastVertexId                         = -1L
      private var endVertexId                          = -1L
      private var lastEdgeId                           = -1L
      private var endEdgeId                            = -1L
      private var lastEdgePartition: EdgePartition     = null
      private var lastVertexPartition: VertexPartition = null

      val vertexIter: VertexIterator = rap.getNewAllVerticesIterator

      getNextVertexId()

      private def getNextVertexId(): Unit = {
        val partId = avpm.getNewPartitionId
        lastVertexId = partId * avpm.PARTITION_SIZE
        endVertexId = lastVertexId + avpm.PARTITION_SIZE
        lastVertexPartition = avpm.getPartition(partId)
      }

      override def onEvent(event: VertexAddEvent, sequence: Long, endOfBatch: Boolean): Unit =
        if (event.dstGlobalId == -1L) addVertex(event) else addEdge(event)

      private def addEdge(event: VertexAddEvent): Unit = {
        val srcId = leis.getLocalNodeId(event.globalId)
        var dstId = leis.getLocalNodeId(event.dstGlobalId)

        while (dstId == -1L) {
          Thread.`yield`()
          dstId = leis.getLocalNodeId(event.dstGlobalId)
          //LockSupport.parkNanos(1L)
        }

        // Check if edge already exists...
        var e    = -1L
        vertexIter.reset(srcId)
        val iter = vertexIter.findAllOutgoingEdges(dstId, false)
        if (iter.hasNext)
          e = iter.next()

        ///System.out.println(av._globalId + "," + av._dstGlobalId + ", " + (e!=null))

        if (e == -1L) {
          val edgeId = addEdge(srcId, dstId, event.time, event.price)
        }
        else {
          priceVEPA.reset()
          priceVEPA.setHistory(true, event.time).set(event.price)
          aepm.addProperty(e, priceProperty, priceVEPA)
          aepm.addHistory(e, event.time, true, true)
          avpm.addHistory(iter.getSrcVertexId, event.time, true, false, e, true)
          avpm.addHistory(iter.getDstVertexId, event.time, true, false, e, false)

          //e.decRefCount()
          //AI_NEDGES_UPDATED.incrementAndGet()
        }
      }

      private def addEdge(srcId: Long, dstId: Long, time: Long, price: Long): Long = {
        if (lastEdgeId == -1L || lastEdgeId >= endEdgeId) {
          val partId = aepm.getNewPartitionId
          lastEdgeId = partId * aepm.PARTITION_SIZE
          endEdgeId = lastEdgeId + aepm.PARTITION_SIZE
          lastEdgePartition = aepm.getPartition(partId)
        }

        val e = rap.getEdge
        e.incRefCount()

        e.init(lastEdgeId, true, time)
        e.resetEdgeData(srcId, dstId, false, false)
        e.getProperty(priceProperty).set(price)

        var p           = avpm.getPartitionForVertex(srcId)
        val outgoingPtr = p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, false)
        p.addHistory(srcId, time, true, false, e.getLocalId, true)

        p = avpm.getPartitionForVertex(dstId)
        val incomingPtr = p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId, e.getSrcVertex)
        p.addHistory(dstId, time, true, false, e.getLocalId, false)

        lastEdgePartition.addEdge(e, incomingPtr, outgoingPtr)
        lastEdgePartition.addHistory(e.getLocalId, time, true, true)

        e.decRefCount()

        //AI_NEDGES_ADDED.incrementAndGet()

        lastEdgeId += 1
        lastEdgeId
      }

      private def addVertex(event: VertexAddEvent): Unit = {
        if (lastVertexId >= endVertexId) getNextVertexId()

        val localId = leis.getLocalNodeId(event.globalId)
        if (localId == -1L) {
          createVertex(lastVertexId, event.globalId, event.nodeId, event.time).decRefCount()
          lastVertexId += 1
        }
      }

      private def createVertex(localId: Long, globalId: Long, nodeId: Long, time: Long): Vertex = {
        val v = rap.getVertex
        v.incRefCount()
        v.reset(localId, globalId, true, time)
        v.getField(nodeIdField).set(nodeId)
        lastVertexPartition.addVertex(v)
        lastVertexPartition.addHistory(localId, time, true, true, -1L, false)
        v
      }
    }

    def loadMT(file: String): Unit = {
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

      var workers    = Array[Worker]()
      var queues     = Array[RingBuffer[VertexAddEvent]]()
      var disruptors = Array[Disruptor[VertexAddEvent]]()

      (0 until N_LOAD_THREADS).foreach { i =>
        val _worker    = new Worker(i)
        val _disruptor = buildDisruptor()
        _disruptor.handleEventsWith(_worker)

        workers = workers :+ _worker
        disruptors = disruptors :+ _disruptor
        queues = queues :+ _disruptor.start()
      }

      def queueVertex(worker: Int, globalId: Long, nodeId: Long, time: Long): Unit = {
        val q = queues(worker)

        val sequenceId = q.next()
        val event      = q.get(sequenceId)
        event.initAddVertex(globalId, nodeId, time)
        q.publish(sequenceId)
      }

      def queueEdge(worker: Int, srcGlobalId: Long, dstGlobalId: Long, time: Long, price: Long): Unit = {
        val q = queues(worker)

        val sequenceId = q.next()
        val event      = q.get(sequenceId)
        event.initAddEdge(srcGlobalId, dstGlobalId, time, price)
        q.publish(sequenceId)
      }

      val br           = new BufferedReader(new InputStreamReader(new FileInputStream(file)), BUFFER_SIZE)
      var line: String = br.readLine()
      var nLines       = 0
      val start        = System.currentTimeMillis()

      println(s"${new Date()}: Starting load")

      val BATCH     = 4096
      val SGBID     = Array.ofDim[Long](BATCH)
      val DGBID     = Array.ofDim[Long](BATCH)
      val TIME      = Array.ofDim[Long](BATCH)
      val PRICE     = Array.ofDim[Long](BATCH)
      var batchSize = 0

      while (line != null) {
        nLines += 1
        if (nLines % (1024 * 1024) == 0) println(nLines)

        val fields = line.split(",")

        val srcGlobalId = rap.getGlobalEntityIdStore.getGlobalNodeId(fields(3))
        val src         = fields(3).toLong

        val dstGlobalId = rap.getGlobalEntityIdStore.getGlobalNodeId(fields(4))
        val dst         = fields(4).toLong

        val time  = fields(5).toLong
        val price = fields(7).toLong

        var srcWorker = Math.abs((srcGlobalId % N_LOAD_THREADS).toInt)
        var dstWorker = Math.abs((dstGlobalId % N_LOAD_THREADS).toInt)

        queueVertex(dstWorker, dstGlobalId, dst, time)
        queueVertex(srcWorker, srcGlobalId, src, time)

        if (true) {
          val bs = batchSize
          SGBID(bs) = srcGlobalId
          DGBID(bs) = dstGlobalId
          TIME(bs) = time
          PRICE(bs) = price

          batchSize += 1
          if (batchSize >= BATCH) {
            (0 until BATCH).foreach { i =>
              srcWorker = Math.abs((SGBID(i) % N_LOAD_THREADS).toInt)
              queueEdge(srcWorker, SGBID(i), DGBID(i), TIME(i), PRICE(i))
            }

            batchSize = 0
          }
        }
        else queueEdge(srcWorker, srcGlobalId, dstGlobalId, time, price)

        line = br.readLine()
      }

      if (batchSize != 0)
        (0 until batchSize).foreach { i =>
          val srcWorker = Math.abs((SGBID(i) % N_LOAD_THREADS).toInt)
          queueEdge(srcWorker, SGBID(i), DGBID(i), TIME(i), PRICE(i))
        }

      println("Waiting...")

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

      val end  = System.currentTimeMillis()
      val rate = rap.getStatistics.getNVertices / ((end - start).toDouble / 1000.0d)

      println(s"${new Date()}: Ending load: rate=$rate per second")
      println(rap.getStatistics)
      println(s"Total time: ${end - start} ms")
    }
  }

  final case class AlphaBaySchema(
      nonversionedVertexProperties: util.ArrayList[NonversionedField] = new util.ArrayList(
              util.Arrays.asList(
                      new NonversionedField("globalid", classOf[java.lang.Long])
              )
      ),
      versionedVertexProperties: util.ArrayList[VersionedProperty] = null,
      nonversionedEdgeProperties: util.ArrayList[NonversionedField] = null,
      versionedEdgeProperties: util.ArrayList[VersionedProperty] = new util.ArrayList(
              util.Arrays.asList(
                      new VersionedProperty("price", classOf[java.lang.Long])
              )
      )
  ) extends PropertySchema

  val cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig()
  cfg._propertySchema = AlphaBaySchema()
  cfg._arrowDir = ARROW_DIR
  cfg._raphtoryPartitionId = 0
  cfg._nRaphtoryPartitions = 1
  cfg._nLocalEntityIdMaps = 128
  cfg._localEntityIdMapSize = 1024
  cfg._syncIDMap = false
  cfg._edgePartitionSize = 1024 * 1024
  cfg._vertexPartitionSize = 256 * 1024

  val loader = new AlphaBayLoader(new RaphtoryArrowPartition(cfg))
  loader.loadMT(s"$DATA_DIR/alphabay_sorted.csv")
}
