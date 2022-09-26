package com.raphtory.api.input

import com.raphtory.internals.graph.GraphBuilderInstance

trait GraphBuilder[T] extends ((Graph, T) => Unit) with Serializable {

  final def buildInstance(graphID: String, sourceID: Int): GraphBuilderInstance[T] =
    new GraphBuilderInstance[T](graphID, sourceID, this)
}

private[raphtory] object GraphBuilder {

  def apply[T](parseFun: (Graph, T) => Unit): GraphBuilder[T] =
    new GraphBuilder[T] { override def apply(graph: Graph, tuple: T): Unit = parseFun(graph, tuple) }
}
