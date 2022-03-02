package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
class GenericGraphPerspective(private val queryBuilder: QueryBuilder) extends GraphPerspective {
  override def filter(f: (Vertex) => Boolean): GraphPerspective = addFunction(VertexFilter(f))

  override def step(f: (Vertex) => Unit): GraphPerspective = addFunction(Step(f))

  override def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = addFunction(Iterate(f, iterations, executeMessagedOnly))

  override def select(f: Vertex => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(Select(f)))

  override def explodeSelect(f: Vertex => List[Row]): Table =
    new GenericTable(queryBuilder.addGraphFunction(ExplodeSelect(f)))

  override def clearMessages(): GraphPerspective =
    addFunction(ClearChain())

  private def addFunction(function: GraphFunction) =
    new GenericGraphPerspective(queryBuilder.addGraphFunction(function))
}
