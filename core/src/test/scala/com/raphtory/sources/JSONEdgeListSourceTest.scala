package com.raphtory.sources

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.sources.JSONEdgeListSource
import com.raphtory.spouts.FileSpout


class JSONEdgeListSourceTest extends BaseCorrectnessTest {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  val path = currentDirectory + "/core/src/test/resources/SourceTest/jsontestdata.json"
  override def setSource() = JSONEdgeListSource(FileSpout(path),sourceKey="name", targetKey = "favourite-game", timeKey = "date")


  withGraph.test("JSON Edge List Source Test") { graph =>
    correctnessTest(
      TestQuery(EdgeList()),
      "SourceTest/jsontestresults.csv",
      graph
    )
  }
}
