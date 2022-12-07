package com.raphtory.sources

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.spouts.FileSpout

class JSONEdgeListSourceTest extends BaseCorrectnessTest {

  val currentDirectory     = new java.io.File(".").getCanonicalPath
  val path                 = currentDirectory + "/it/src/it/resources/SourceTest/jsontestdata.json"

  override def setSource() =
    JSONEdgeListSource(FileSpout(path), sourceKey = "name", targetKey = "favourite-game", timeKey = "date")

  test("JSON Edge List Source Test") {
    correctnessTest(
            TestQuery(EdgeList()),
            "SourceTest/jsontestresults.csv"
    )
  }
}
