package com.raphtory.core.algorithm

/**
  * {s}`GraphPerspective`
  *  : Public interface for graph algorithms
  *
  * The {s}`GraphPerspective` is the core interface of the algorithm API. It implements the operations exposed
  * by {s}`GraphOperations` returning a new {s}`GraphPerspective` for those operations that have a graph as a result.
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.GraphOperations)
  * ```
  */
trait GraphPerspective extends GraphOperations[GraphPerspective]
