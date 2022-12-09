package com.test.raphtory.algorithms

import com.raphtory.algorithms.filters.VertexFilter
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.{BaseCorrectnessTest, TestQuery, TestUtils}
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout
import com.test.raphtory.{AllNeighbours, ShitVertexFilter}

import scala.io.Source
import scala.util.Using

class VertexFilterTest extends BaseCorrectnessTest with Serializable{

  override def munitIgnore: Boolean = runningIntegrationTest

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
