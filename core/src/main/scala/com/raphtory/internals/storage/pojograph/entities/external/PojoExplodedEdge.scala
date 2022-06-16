package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteExplodedEdge
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[raphtory] class PojoExplodedEdge(
    objectEdge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    val src: Long,
    val dst: Long,
    override val timestamp: Long
) extends PojoExEntity(objectEdge, view)
        with PojoExEdgeBase[Long]
        with ConcreteExplodedEdge[Long] {
  override type ExplodedEdge = PojoExplodedEdge

  override def explode(): List[ExplodedEdge] = List(this)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[List[(Long, T)]] = super.getPropertyHistory(key, after, before)
}

private[raphtory] object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timestamp: Long): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            pojoExEdge.src,
            pojoExEdge.dst,
            timestamp
    )
}
