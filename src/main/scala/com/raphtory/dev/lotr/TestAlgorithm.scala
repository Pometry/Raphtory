package com.raphtory.dev.lotr

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TestAlgorithm extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step(v=> v.setState("test","test"))
      .iterate(v=>v.setState("test2","test2"),5)
      .select(v=> Row(v.ID(),v.getState[String]("test")))
      .filter(v=> true)
      .writeTo("")
  }
}
object TestAlgorithm{
  def apply() = new TestAlgorithm()
}
