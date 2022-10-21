package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.SquareCount
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource

class SquareCountTest extends BaseCorrectnessTest {
  withGraph.test("test PQ square") { graph =>
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squarePQ.csv",
            "SquareCount/singleSquareResult.csv",
            graph
    )
  }

  withGraph.test("test PR square") { graph =>
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squarePR.csv",
            "SquareCount/singleSquareResult.csv",
            graph
    )
  }

  withGraph.test("test QR square") { graph =>
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squareQR.csv",
            "SquareCount/singleSquareResult.csv",
            graph
    )
  }

  withGraph.test("test combined example") { graph =>
    correctnessTest(
            TestQuery(SquareCount, 19),
            "SquareCount/squareTestCorrectResult.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource.fromResource("SquareCount/squareTest.csv")
}
