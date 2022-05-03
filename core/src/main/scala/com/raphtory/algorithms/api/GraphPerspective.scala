package com.raphtory.algorithms.api

/** Core Public interface of the algorithm API
  * This implements the operations exposed by GraphOperations returning
  * a new GraphPerspective for those operations that have a graph as a result.
  *
  * @see [[com.raphtory.algorithms.api.GraphOperations]]
  */
trait GraphPerspective extends GraphOperations[GraphPerspective]
