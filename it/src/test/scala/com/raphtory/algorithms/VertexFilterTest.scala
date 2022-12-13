package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.filters.VertexFilter
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.input
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.{ResourceOrFileSpout, ResourceSpout}

import scala.io.Source
import scala.util.Using

object AllNeighbours extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex.edges.map(e => Row(vertex.name(), e.src, e.dst))
    }
}

class VertexFilterTest extends BaseCorrectnessTest {

  override def munitIgnore: Boolean = System.getenv("IT_OUTPUT_PATH") != null

  test("Vertex is being filtered") {
    val res = Using(Source.fromResource("MotifCount/motiftest.csv")) { source =>
      source
        .getLines()
        .flatMap { s =>
          val parts = s.split(",").map(_.trim)
          if (parts(0) == "1" || parts(1) == "1")
            List.empty
          else
            List(s"23,${parts(0)},${parts(0)},${parts(1)}", s"23,${parts(1)},${parts(0)},${parts(1)}")
        }
        .toList
    }.get

    correctnessTest(
            TestQuery(VertexFilter(_.ID != 1) -> AllNeighbours, 23),
            "MotifCount/motiftest.csv",
            res
    )
  }

  override def setSource() = CSVEdgeListSource(ResourceOrFileSpout("MotifCount/motiftest.csv"))
}
