package com.raphtory.sources

import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source

class CSVEdgeListSourceTest extends BaseCorrectnessTest {

  test("CSV Edge List Source Test without headers") {
    correctnessTest(
            TestQuery(EdgeList()),
            "SourceTest/csvtestdata.csv",
            "SourceTest/csvtestdataresults.csv"
    )
  }

  test("CSV Edge List Source Test with headers") {
    correctnessTest(
            TestQuery(EdgeList()),
            "SourceTest/csvtestdataresults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource.fromResource("SourceTest/csvtestdataheaders.csv", header = true)
}
