package com.raphtory.internals.storage

import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Vertex

import scala.annotation.implicitNotFound
import scala.collection.View
package object arrow {

  implicit class RichVertex(val v: Vertex) extends AnyVal {

    def prop[P: Prop](name: String): PropAccess[P] =
      new PropAccess[P] {

        override def set(p: P): Unit = {
          val FIELD = v.getPartition.getVertexFieldId(name)
          implicitly[Prop[P]].set(v.getField(FIELD), p)
        }

        override def get: P = {
          val FIELD = v.getPartition.getVertexFieldId(name)
          implicitly[Prop[P]].get(v.getField(FIELD))
        }
      }

    def outgoingEdges: View[Edge] = {
      val edgesIter = v.getPartition.getNewAllEdgesIterator
      edgesIter.reset(v.getOutgoingEdgePtr)
      View.from(new ArrowPartition.EdgesIterator(edgesIter))

    }

    def incomingEdges: Iterable[Edge] = {
      val edgesIter = v.getPartition.getNewAllEdgesIterator
      edgesIter.reset(v.getIncomingEdgePtr)
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
    }

  }

  implicit class RichEdge(val v: Edge) extends AnyVal {

    def prop[P: Prop](name: String): PropAccess[P] =
      new PropAccess[P] {

        override def set(p: P): Unit = {
          val FIELD = v.getPartition.getEdgeFieldId(name)
          implicitly[Prop[P]].set(v.getField(FIELD), p)
        }

        override def get: P = {
          val FIELD = v.getPartition.getEdgeFieldId(name)
          implicitly[Prop[P]].get(v.getField(FIELD))
        }
      }

  }
}

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Prop[P] {
  def set(efa: EntityFieldAccessor, v: P): Unit
  def get(efa: EntityFieldAccessor): P
}

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Field[P] {
  def set(acc: VersionedEntityPropertyAccessor, v: P): Unit
  def get(acc: VersionedEntityPropertyAccessor): P
}

object Prop {

  implicit val strProp: Prop[String] = new Prop[String] {

    override def set(efa: EntityFieldAccessor, p: String): Unit =
      efa.set(new java.lang.StringBuilder(p))

    override def get(efa: EntityFieldAccessor): String =
      efa.getString.toString
  }
}

trait PropAccess[P] {
  def set(p: P): Unit
  def get: P
}
