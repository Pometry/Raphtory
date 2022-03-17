package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder

/**
  * @DoNotDocument
  */
class GenericGraphPerspective(queryBuilder: QueryBuilder)
        extends DefaultGraphOperations[GraphPerspective](queryBuilder)
        with GraphPerspective {

  override protected def newGraph(queryBuilder: QueryBuilder) =
    new GenericGraphPerspective(queryBuilder)
}
