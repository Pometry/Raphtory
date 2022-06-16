package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.SquareCount

class SquareCountTest extends BaseCorrectnessTest {
  test("test PQ square") {
    assert(
            correctnessTest(
                    TestQuery(SquareCount, 4),
                    "SquareCount/squarePQ.csv",
                    "SquareCount/singleSquareResult.csv"
            )
    )
  }

  test("test PR square") {
    assert(
            correctnessTest(
                    TestQuery(SquareCount, 4),
                    "SquareCount/squarePR.csv",
                    "SquareCount/singleSquareResult.csv"
            )
    )
  }

  test("test QR square") {
    assert(
            correctnessTest(
                    TestQuery(SquareCount, 4),
                    "SquareCount/squareQR.csv",
                    "SquareCount/singleSquareResult.csv"
            )
    )
  }

  test("test combined example") {
    assert(
            correctnessTest(
                    TestQuery(SquareCount, 19),
                    "SquareCount/squareTest.csv",
                    "SquareCount/squareTestCorrectResult.csv"
            )
    )
  }

}
