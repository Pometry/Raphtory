package com.raphtory.graph.visitor

import scala.runtime.RichLong

/**
  * {s}`ExplodedEdge`
  *   : trait representing a view of an edge at a given point in time
  *
  * An exploded edge represents an [{s}`Edge`](com.raphtory.graph.visitor.Edge) at a particular time point
  * and combines the [{s}`Edge`](com.raphtory.graph.visitor.Edge) and
  * [{s}`ExplodedEntityVisitor`](com.raphtory.graph.visitor.ExplodedEntityVisitor) traits.
  *
  * ```{seealso}
  * [](com.raphtory.graph.visitor.Edge),
  * [](com.raphtory.graph.visitor.ExplodedEntityVisitor)
  * ```
  */
trait ExplodedEdge extends Edge with ExplodedEntityVisitor
