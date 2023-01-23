package com.raphtory.api.analysis.graphview

import com.raphtory.BaseCorrectnessTest
import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.SequenceSpout

/** This should call methods on graph view and make sure they work */
class GraphViewTest extends BaseCorrectnessTest {
  override def setSource(): Source = CSVEdgeListSource(SequenceSpout("1, 1, 1"))

  test("Test explodeSelect with graph state") {
    assertEquals(
            graph
              .setGlobalState(state => state.newConstant("test", 2))
              .explodeSelect((vertex, state) => List(Row(("vertexID", vertex.ID), ("test", state("test").value))))
              .get()
              .next()
              .rows
              .toSeq,
            Seq(Row(("vertexID", 1), ("test", 2)))
    )
  }
}
