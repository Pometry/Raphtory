package com.raphtory.typedb

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.sinks.FileSink

object TypeDBSpoutTest {

  def main(args: Array[String]): Unit = {
    val spout        = new TypeDBSpout("")
    val graphBuilder = ???
    val graph        = Raphtory.stream[String](spout, graphBuilder)
    graph
      .walk("5 milliseconds")
      .window("5 milliseconds")
      .execute(EdgeList())
      .writeTo(FileSink("EdgeList1"))
  }

}
