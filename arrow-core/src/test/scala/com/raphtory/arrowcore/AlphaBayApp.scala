package com.raphtory.arrowcore

import com.raphtory.arrowcore.implementation._
import com.raphtory.arrowcore.model._

import java.io._
import java.util
import java.util.Date

object AlphaBayApp extends App {

  val DATA_DIR  = "/home/jatinder/projects/Pometry/arrow-core"
  val ARROW_DIR = "/tmp/alphabay/arrow"

  class AlphaBayLoader(rap: RaphtoryArrowPartition) {
    private val BUFFER_SIZE   = 64 * 1024
    private var nEdgesAdded   = 0
    private var nEdgesUpdated = 0

    private val aepm = rap.getEdgeMgr
    private val avpm = rap.getVertexMgr
    private val vItr = rap.getNewAllVerticesIterator

    private val nodeIdField: Int                           = rap.getVertexFieldId("globalid")
    private val priceProperty: Int                         = rap.getEdgePropertyId("price")
    private val priceVEPA: VersionedEntityPropertyAccessor = rap.getEdgePropertyAccessor(priceProperty)

    private def createVertex(localId: Long, globalId: Long, nodeId: Long, time: Long): Vertex = {
      val v = rap.getVertex
      v.incRefCount()
      v.reset(localId, globalId, true, time)
      v.getField(nodeIdField).set(nodeId)
      avpm.addVertex(v)
      avpm.addHistory(localId, time, true, true, -1L, false)
      v
    }

    private def addVertex(globalId: Long, nodeId: Long, time: Long): Unit = {
      val localId = rap.getLocalEntityIdStore.getLocalNodeId(globalId)
      if (localId == -1L) {
        val id = avpm.getNextFreeVertexId
        val v  = createVertex(id, globalId, nodeId, time)
        v.decRefCount()
      }
    }

    private def addEdge(srcId: Long, dstId: Long, time: Long, price: Long): Unit = {
      val e = rap.getEdge
      e.incRefCount()

      val edgeId = aepm.getNextFreeEdgeId
      e.init(edgeId, true, time);

      e.resetEdgeData(srcId, dstId, false, false)
      e.getProperty(priceProperty).set(price)

      aepm.addEdge(e, -1L, -1L);
      val ep = aepm.getPartition(aepm.getPartitionId(e.getLocalId))
      ep.addHistory(e.getLocalId, time, true, true)

      var p = avpm.getPartitionForVertex(srcId)
      ep.setOutgoingEdgePtrByEdgeId(
              e.getLocalId,
              p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, false)
      )
      p.addHistory(srcId, time, true, false, e.getLocalId, true)

      p = avpm.getPartitionForVertex(dstId)
      ep.setIncomingEdgePtrByEdgeId(e.getLocalId, p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId, e.getSrcVertex))
      p.addHistory(dstId, time, true, false, e.getLocalId, false)

      e.decRefCount()

      nEdgesAdded += 1
    }

    private def addOrUpdateEdge(src: Long, dst: Long, time: Long, price: Long) = {
      val srcId = rap.getLocalEntityIdStore.getLocalNodeId(src)
      val dstId = rap.getLocalEntityIdStore.getLocalNodeId(dst)

      // Check if edge already exists...
      vItr.reset(srcId)
      val iter = vItr.findAllOutgoingEdges(dstId, false)
      var e    = -1L
      if (iter.hasNext) e = iter.next()

      if (e == -1L) addEdge(srcId, dstId, time, price)
      else {
        nEdgesUpdated += 1
        priceVEPA.reset()
        priceVEPA.setHistory(true, time).set(price)
        aepm.addProperty(e, priceProperty, priceVEPA)
        aepm.addHistory(e, time, true, true)
        avpm.addHistory(iter.getSrcVertexId, time, true, false, e, true)
        avpm.addHistory(iter.getDstVertexId, time, true, false, e, false)
      }
    }

    def load(file: String): Unit = {
      println(s"${new Date()}: Starting load")

      val br: BufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)), BUFFER_SIZE)
      var line: String       = br.readLine()
      var nLines             = 0
      val start              = System.currentTimeMillis()
      while (line != null) {
        nLines += 1
        if (nLines % (1024 * 1024) == 0) println(nLines)

        val fields = line.split(",")

        val srcGlobalId = rap.getGlobalEntityIdStore.getGlobalNodeId(fields(3))
        val src         = fields(3).trim().toLong

        val dstGlobalId = rap.getGlobalEntityIdStore.getGlobalNodeId(fields(4))
        val dst         = fields(4).trim().toLong

        val time  = fields(5).trim().toLong
        val price = fields(7).trim().toLong

        //addVertex(srcGlobalId, src, time)
        //addVertex(dstGlobalId, dst, time)
        //addOrUpdateEdge(srcGlobalId, dstGlobalId, time, price)

        line = br.readLine()
      }

      val end  = System.currentTimeMillis()
      val rate = rap.getStatistics.getNVertices / ((end - start).toDouble / 1000.0d)

      println(s"${new Date()}: Ending load: rate=$rate per second")
      println(rap.getStatistics)
      println(s"nEdgesAdded=$nEdgesAdded, nEdgesUpdated=$nEdgesUpdated")
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
  loader.load(s"$DATA_DIR/alphabay_sorted.csv")
}
