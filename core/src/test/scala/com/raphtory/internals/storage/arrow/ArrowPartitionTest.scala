package com.raphtory.internals.storage.arrow

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.Properties
import com.raphtory.arrowcore.implementation.RaphtoryThreadPool
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.communication.SchemaProviderInstances.genericSchemaProvider
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Scheduler
import munit.FunSuite

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.ArrayBlockingQueue
import java.util.zip.GZIPInputStream

class ArrowPartitionTest extends FunSuite {

  test("load lotr data then look for Aragorn".only) {

    val file   = "/tmp/lotr.csv"
    val parOut = Paths.get("/pometry/wip/lotr_arrow_data")
    val br     = new BufferedReader(new InputStreamReader(new FileInputStream(file)), 65536);

    val config    = ConfigBuilder.getDefaultConfig
    val par       = ArrowPartition(
            "testos",
            ArrowPartitionConfig(
                    config,
                    0,
                    ArrowSchema[VertexProp, EdgeProp],
                    parOut
            ),
            config
    )
    var i         = 1
    val emptyProp = Properties()
    br.lines().forEach { tuple =>
      val fileLine   = tuple.split(",").map(_.trim)
      val sourceNode = fileLine(0)
      val srcID      = assignID(sourceNode)
      val targetNode = fileLine(1)
      val tarID      = assignID(targetNode)
      val time       = fileLine(2).toLong

      val add1  = VertexAdd(1, time, i, srcID, Properties(ImmutableString("name", sourceNode)), None)
      par.addVertex(add1)
      val add2  = VertexAdd(1, time, i, tarID, Properties(ImmutableString("name", targetNode)), None)
      par.addVertex(add2)
      val eAdd1 = EdgeAdd(1, time, i, srcID, tarID, emptyProp, None)
      par.addLocalEdge(eAdd1)
      i += 1

    }

    par.flush

    val window = 10001
    val start  = window - 500
    val end    = window
    val names  = par.windowVertices(start, end).flatMap(v => v.field[String]("name").get).toVector.sorted

    assertEquals(
            names,
            Vector(
                    "Aragorn",
                    "Balin",
                    "Bilbo",
                    "Boromir",
                    "Daeron",
                    "Frodo",
                    "Fundin",
                    "Gandalf",
                    "Gimli",
                    "Legolas",
                    "Merry",
                    "Ori",
                    "Pippin",
                    "Sam",
                    "Thorin"
            ).sorted
    )

    val w = start.toLong until end.toLong

    val degrees1 = par
      .windowVertices(start, end)
      .map(v => (v.field[String]("name").get.get, v.outgoingEdges.count(e => w.contains(e.getCreationTime)), v.incomingEdges.count(e => w.contains(e.getCreationTime))))
      .toVector.sorted

    println(degrees1)

    // 12 ends up being Aragorn
    val localId = par.par.getLocalEntityIdStore.getLocalNodeId(929951733927649394L)
    val vPar = par.par.getVertexMgr.getPartitionForVertex(localId)

    val vIter = par.par.getNewWindowedVertexIterator(start, end)
    vIter.reset(localId)

    vIter.hasNext
    vIter.next()
    val v = vIter.getVertex

    par.par.getNewWindowedEdgeIterator(start, end)

    val q = new ArrayBlockingQueue[Long](10)
    val manager = par.par.getNewMTWindowedVertexManager(RaphtoryThreadPool.THREAD_POOL, start, end)
    manager.start{
      (_, vi) =>{

        while (vi.hasNext) {

          vi.next()
          if (vi.getGlobalVertexId == 929951733927649394L) {

            val ei = vi.getOutgoingEdges
            while (ei.hasNext) {
              ei.next()
              println("SUGI")
              val e = ei.getEdge
              q.offer(e.getDstVertex)
            }

            val ei2 = vi.getIncomingEdges
            while (ei2.hasNext) {
              ei2.next()
              println("PULA")
              val e = ei2.getEdge
              q.offer(e.getSrcVertex)
            }
          }
        }
      }
    }

    manager.waitTilComplete()

    println(q)

    val degrees = par
      .windowVertices(start, end)
      .map { v =>
        (
                v.field[String]("name").get.get,
                v.outgoingEdges.map(_.getCreationTime).toVector.sorted,
                v.incomingEdges.map(_.getCreationTime).toVector.sorted
        )
      }
      .toVector
      .sortBy(_._1)

    degrees.foreach(println)
    val lens = par.lens("a", start, end, 1, _ => (), t => t.printStackTrace(), new Scheduler)
  }

  test("how fast can we load from one single gzipped file on a single Arrow Partition") {

    val file   = "/pometry/wip/alphabay_sorted.csv.gz"
    val parOut = Paths.get("/pometry/wip/scala_arrow_data")
    val br     = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file), 65536)), 65536);

    val config    = ConfigBuilder.getDefaultConfig
    val par       = ArrowPartition(
            "testos",
            ArrowPartitionConfig(
                    config,
                    0,
                    ArrowSchema[VertexProp, EdgeProp],
                    parOut
            ),
            config
    )
    var i         = 1
    val emptyProp = Properties()
    val start     = LocalDateTime.now()
    br.lines().forEach { line =>
      val fields      = line.split(",");
      val srcGlobalId = fields(3).toLong
      val dstGlobalId = fields(4).toLong
      val time        = fields(5).toLong
      val price       = fields(7).toLong

      val add1  = VertexAdd(1, time, i, srcGlobalId, emptyProp, None)
      par.addVertex(add1)
      val add2  = VertexAdd(1, time, i, dstGlobalId, emptyProp, None)
      par.addVertex(add2)
      val eAdd1 = EdgeAdd(1, time, i, srcGlobalId, dstGlobalId, Properties(MutableLong("weight", price)), None)
      par.addLocalEdge(eAdd1)
      i += 1

    }

    par.flush

    val end = LocalDateTime.now()

    println(
            s"Loading All of AlphaBay too ${Duration.between(start, end).toSeconds}s also known as ${Duration.between(start, end).toMinutes}min"
    )

  }
}
