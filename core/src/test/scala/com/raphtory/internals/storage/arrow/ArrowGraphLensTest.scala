package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.visitor
import com.raphtory.api.input.Properties
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.components.partition.EdgeProp
import com.raphtory.internals.components.partition.VertexProp
import com.raphtory.internals.graph.GraphAlteration.SyncExistingEdgeAdd
import com.raphtory.internals.graph.GraphAlteration.SyncNewEdgeAdd
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import munit.FunSuite

import java.nio.file.Files
import scala.collection.View
import scala.util.Using

class ArrowGraphLensTest extends FunSuite {

  private def mockCluster() =
    MockCluster(
            ConfigBuilder()
              .addConfig("raphtory.partitions.countPerServer", "4")
              .addConfig("raphtory.partitions.serverCount", "1")
              .config
    )

  private val data = Vector(
          (1, 2, 1),
          (1, 3, 2),
          (1, 4, 3),
          (3, 1, 4),
          (3, 4, 5),
          (3, 5, 6),
          (4, 5, 7),
          (5, 6, 8),
          (5, 8, 9),
          (7, 5, 10),
          (8, 5, 11),
          (1, 9, 12),
          (9, 1, 13),
          (6, 3, 14),
          (4, 8, 15),
          (8, 3, 16),
          (5, 10, 17),
          (10, 5, 18),
          (10, 8, 19),
          (1, 11, 20),
          (11, 1, 21),
          (9, 11, 22),
          (11, 9, 23)
  )

  test("filter one edge should result in it missing from the next step") {
    Using(mockCluster()) { cluster =>
      data.foreach {
        case (src, dst, time) =>
          cluster.addTriplet(src, dst, time)
      }

      assertEquals(cluster.vertices.toList.sorted, (1L to 11L).toList.sorted)
      assertEquals(
              cluster.verticesPerPartition,
              Vector(Vector[Long](4, 8), Vector[Long](1, 5, 9), Vector[Long](2, 6, 10), Vector[Long](3, 7, 11))
      )
    }
  }

  test("vertices over windowed iterator with Long.Min Long.Max should be the same as min(t),max(t) iterator") {

    Using(mockCluster()) { cluster =>
      data.foreach {
        case (src, dst, time) =>
          cluster.addTriplet(src, dst, time)
      }

      val maximalWindowVertices = cluster.vertices(1L, 23L).map(_.ID).toVector
      val allVertices           = cluster.vertices(Long.MinValue, Long.MaxValue).map(_.ID).toVector
      assertEquals(maximalWindowVertices, allVertices)
    }
  }

  test("edges over windowed iterator with Long.Min Long.Max should be the same as min(t),max(t) iterator") {

    Using(mockCluster()) { cluster =>
      data.foreach {
        case (src, dst, time) =>
          cluster.addTriplet(src, dst, time)
      }

      val maximalWindowVertices: Map[Any, Vector[Any]] =
        cluster
          .vertices(Long.MinValue, Long.MaxValue)
          .map(v => v.ID -> v.neighbours.toVector.sorted(v.IDOrdering))
          .toMap

      val allVertices: Map[Any, Vector[Any]] = cluster
        .vertices(Long.MinValue, Long.MaxValue)
        .map(v => v.ID -> v.neighbours.toVector.sorted(v.IDOrdering))
        .toMap

      assertEquals(allVertices, maximalWindowVertices)

      assertEquals(maximalWindowVertices.getOrElse(1L, Vector.empty), Vector(2, 3, 4, 9, 11))
      assertEquals(maximalWindowVertices.getOrElse(6L, Vector.empty), Vector(3, 5))
      assertEquals(maximalWindowVertices.getOrElse(8L, Vector.empty), Vector(3, 4, 5, 10))
    }.get
  }

}

class MockCluster(parts: Vector[ArrowPartition]) extends AutoCloseable {

  def vertices: View[Long] = parts.view.flatMap(_.vertices).map(_.getGlobalId)

  def verticesPerPartition: Vector[Vector[Long]] = parts.view.map(_.vertices.toVector.map(_.getGlobalId)).toVector

  def addTriplet(src: Long, dst: Long, time: Long): Unit = {
    addVertex(src, time)
    addVertex(dst, time)
    addEdge(src, dst, time)
  }

  def addEdge(src: Long, dst: Long, time: Long): Unit = {
    val srcPar = findPartition(src)
    srcPar.addEdge(-1, time, -1, src, dst, Properties(), None) match {
      case Some(SyncExistingEdgeAdd(_, updateTime, _, srcId, dstId, properties))  =>
        val dstPar = findPartition(dst)
        dstPar.syncExistingEdgeAdd(-1, updateTime, -1, srcId, dstId, properties)
      case Some(SyncNewEdgeAdd(_, updateTime, _, srcId, dstId, properties, _, _)) =>
        val dstPar = findPartition(dst)
        dstPar.syncNewEdgeAdd(-1, updateTime, -1, srcId, dstId, properties, Nil, None)
      case None                                                                   =>
    }
  }

  def addVertex(v: Long, time: Long): Unit = {
    val par: ArrowPartition = findPartition(v)

    par.addVertex(-1, time, -1, v, Properties(), None)
  }

  private def findPartition(src: Long) =
    parts.zipWithIndex
      .find {
        case (par, id) =>
          GraphPartition.checkDst(src, parts.size, id)
      }
      .map(_._1)
      .get

  override def close(): Unit =
    parts.foreach(_.close())

  def vertices(start: Long, end: Long): View[visitor.Vertex] =
    parts.view
      .map(par => par.lens("a", start, end, 1, _ => (), t => (), new Scheduler()))
      .collect {
        case lens: ArrowGraphLens =>
          lens
      }
      .flatMap(_.vertices)

}

object MockCluster {
  private val defaultPropSchema = ArrowSchema[VertexProp, EdgeProp]

  def apply(config: Config): MockCluster = {
    val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
    val parts                    = (0 until partitionsPerServer).map { partId =>
      val cfg = ArrowPartitionConfig(
              config,
              partId,
              defaultPropSchema,
              Files.createTempDirectory(s"part-$partId")
      )
      ArrowPartition("test-graph-1", cfg, config)
    }.toVector
    new MockCluster(parts)
  }
}
