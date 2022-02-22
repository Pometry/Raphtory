package com.raphtory.algorithms


import com.raphtory.algorithms.generic.motif.SquareCount


class SquareCountTest extends BaseCorrectnessTest {
  test("test PQ square") {
    assert(correctnessTest(SquareCount(), "SquareCount/squarePQ.csv", "SquareCount/singleSquareResult.csv", 4))
  }

  test("test PR square"){
    assert(correctnessTest(SquareCount(),"SquareCount/squarePR.csv", "SquareCount/singleSquareResult.csv", 4))
  }

  test("test QR square"){
    assert(correctnessTest(SquareCount(),"SquareCount/squareQR.csv", "SquareCount/singleSquareResult.csv", 4))
  }

  test("test combined example"){
    assert(correctnessTest(SquareCount(),"SquareCount/squareTest.csv", "SquareCount/squareTestCorrectResult.csv", 19))
  }

}

