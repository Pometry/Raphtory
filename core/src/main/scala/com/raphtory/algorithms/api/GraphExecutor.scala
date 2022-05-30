package com.raphtory.algorithms.api

import com.raphtory.algorithms.api.algorithm.BaseGraphAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericallyApplicableAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerProjectionAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericReductionAlgorithm
import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.ExplodedVertex
import com.raphtory.graph.visitor.Vertex

abstract class GraphExecutor[
    V <: Vertex,
    G <: GraphExecutor[V, G, RG, MG],
    RG <: GraphExecutor[Vertex, RG, RG, MG],
    MG <: MultilayerGraphExecutor[MG, RG]
](
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender
) extends DefaultGraphOperations[V, G, RG, MG]
        with GraphPerspective[G] { this: G =>

  /**  Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.
    *  @param algorithm algorithm to apply
    */
  def transform(algorithm: GenericAlgorithm): G =
    algorithm
      .apply(newGraph(query.copy(name = transformedName(algorithm)), querySender))
      .clearMessages()

  def transform(algorithm: MultilayerProjectionAlgorithm): MG =
    algorithm(newGraph(query.copy(name = transformedName(algorithm)), querySender)).clearMessages()

  def transform(algorithm: GenericReductionAlgorithm): RG =
    algorithm(newGraph(query.copy(name = transformedName(algorithm)), querySender)).clearMessages()

  /** Execute the algorithm on every perspective and returns a new `RaphtoryGraph` with the result.
    * @param algorithm to apply
    */
  def execute(algorithm: GenericallyApplicableAlgorithm): Table =
    algorithm.run(newGraph(query.copy(name = transformedName(algorithm)), querySender))

  /** Apply f over itself and return the result. `graph.execute(f)` is equivalent to `f(graph)`
    *  @param f function to apply
    */
  def execute(f: G => Table): Table = f(this)

  private[api] def transformedName(algorithm: BaseGraphAlgorithm) =
    query.name match {
      case "" => algorithm.name
      case _  => query.name + "->" + algorithm.name
    }
}

abstract class MultilayerGraphExecutor[
    G <: MultilayerGraphExecutor[G, RG],
    RG <: GraphExecutor[Vertex, RG, RG, G]
](
    query: Query,
    querySender: QuerySender
) extends GraphExecutor[ExplodedVertex, G, RG, G](query, querySender)
        with DefaultMultilayerGraphOperations[G, RG] { this: G =>

  def transform(algorithm: MultilayerAlgorithm): G =
    algorithm(newGraph(query.copy(name = transformedName(algorithm)), querySender)).clearMessages()

  def execute(algorithm: MultilayerAlgorithm): Table =
    algorithm.run(newGraph(query.copy(name = transformedName(algorithm)), querySender))
}
