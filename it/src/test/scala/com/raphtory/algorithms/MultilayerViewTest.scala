package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.temporal.views.MultilayerView
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.SequenceSpout
import com.test.raphtory.WriteValue

class MultilayerViewTest extends BaseCorrectnessTest {
  override def munitIgnore: Boolean = runningIntegrationTest

  test("test multilayer view") {
    correctnessTest(
            TestQuery(MultilayerView() -> EdgeList(), 2),
            Seq("2,1_1,2_1", "2,2_2,1_2")
    )
  }

  test("test temporal node list") {
    correctnessTest(
            TestQuery(TemporalNodeList(), 2),
            Seq[String]("2,1,1", "2,1,2", "2,2,1", "2,2,2")
    )
  }

  test("test temporal edge list") {
    correctnessTest(TestQuery(TemporalEdgeList(), 2), Seq("2,1,2,1", "2,2,1,2"))
  }

  test("test property merging") {
    correctnessTest(
            TestQuery(WriteValue(), 2),
            Seq("2,1,2", "2,2,2")
    )
  }

  override def setSource(): Source = CSVEdgeListSource(SequenceSpout(Seq("1,2,1", "2,1,2")))
}
