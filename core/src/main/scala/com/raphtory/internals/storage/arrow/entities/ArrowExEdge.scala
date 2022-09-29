package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.api.analysis.visitor.{Edge => REdge}
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
import com.raphtory.internals.storage.arrow.ArrowGraphLens
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository

class ArrowExEdge(edge: Edge, protected val repo: ArrowEntityStateRepository)
        extends ConcreteEdge[Long]
        with ArrowExEntity {

  /** type of vertex IDs for this edge */
  override type IDType = Long

  /** Edge ID */
  override def ID: Long = entity.getGlobalId

  /** ID of the source vertex of the edge */
  override def src: Long =
    if (!edge.isSrcGlobal)
      repo.asGlobal(edge.getSrcVertex)
    else edge.getSrcVertex

  /** ID of the destination vertex of the edge */
  override def dst: Long =
    if (!edge.isDstGlobal)
      repo.asGlobal(edge.getDstVertex)
    else edge.getDstVertex

  /** Filter the edge from the `GraphPerspective`. */
  override def remove(): Unit =
    repo.removeEdge(edge.getGlobalId)

  /** Send a message to the vertex connected on the other side of the edge
    *
    * @param data Message data to send
    */
  override def send(data: Any): Unit = ???

  override def entity: Entity = edge
}
