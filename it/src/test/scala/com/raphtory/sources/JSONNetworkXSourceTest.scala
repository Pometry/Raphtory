package com.raphtory.sources

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.spouts.FileSpout

class JSONNetworkXSourceTest extends BaseCorrectnessTest {
  val currentDirectory = new java.io.File(".").getCanonicalPath
  val path             = currentDirectory + "/src/test/resources/SourceTest/lotrnetworkx.json"

  override def setSource(): Source = JSONSource(FileSpout(path))

  test("LOTR NetworkX JSON Test") {
    correctnessTest(
            TestQuery(EdgeList()),
            "SourceTest/lotrnetworkxresults.csv"
    )
  }
}
