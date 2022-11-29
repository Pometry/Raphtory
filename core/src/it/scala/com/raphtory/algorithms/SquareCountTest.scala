package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.SquareCount
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource

class SquareCountTest extends BaseCorrectnessTest {
  test("test PQ square") {
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squarePQ.csv",
            "SquareCount/singleSquareResult.csv"
    )
  }

  test("test PR square") {
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squarePR.csv",
            "SquareCount/singleSquareResult.csv"
    )
  }

  test("test QR square") {
    correctnessTest(
            TestQuery(SquareCount, 4),
            "SquareCount/squareQR.csv",
            "SquareCount/singleSquareResult.csv"
    )
  }

  test("test combined example") {
    correctnessTest(
            TestQuery(SquareCount, 19),
            "SquareCount/squareTestCorrectResult.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource.fromResource("SquareCount/squareTest.csv")
}
