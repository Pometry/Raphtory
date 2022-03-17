package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder

/**
  * @DoNotDocument
  */
class RaphtoryGraphBuilder(queryBuilder: QueryBuilder)
        extends GraphOperationsBuilder[RaphtoryGraph](queryBuilder)
        with RaphtoryGraph {

  override def transform(algorithm: GraphAlgorithm): RaphtoryGraph = {
    val graph            = new GenericGraphPerspective(queryBuilder)
    val transformedGraph = algorithm.apply(graph)
    newGraph(transformedGraph.asInstanceOf[GenericGraphPerspective].queryBuilder)
  }

  override def execute(algorithm: GraphAlgorithm): Table =
    algorithm.run(new GenericGraphPerspective(queryBuilder))

  override protected def newGraph(queryBuilder: QueryBuilder): RaphtoryGraph =
    new RaphtoryGraphBuilder(queryBuilder)
}
