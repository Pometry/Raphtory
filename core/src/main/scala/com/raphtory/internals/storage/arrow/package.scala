package com.raphtory.internals.storage

import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
import com.raphtory.arrowcore.model.{Edge, Entity, Vertex}

import scala.annotation.implicitNotFound
import scala.collection.View
package object arrow {


  trait RichEntityLike {
    def v: Entity

    def field[P: Field](name: String): FieldAccess[P] =
      new FieldAccess[P] {

        override def set(p: P): Unit = {
          val FIELD = v.getRaphtory.getVertexFieldId(name.toLowerCase())
          implicitly[Field[P]].set(v.getField(FIELD), p)
        }

        override def get: P = {
          val FIELD = v.getRaphtory.getVertexFieldId(name.toLowerCase())
          implicitly[Field[P]].get(v.getField(FIELD))
        }
      }

    def prop[P: Prop](name: String): PropAccess[P] =
      new PropAccess[P] {

        override def set(p: P, at: Long): Unit = {
          val FIELD = v.getRaphtory.getVertexPropertyId(name.toLowerCase())
          implicitly[Prop[P]].set(v.getProperty(FIELD), p, at)
        }

        override def get: P = {
          val FIELD = v.getRaphtory.getVertexPropertyId(name.toLowerCase())
          implicitly[Prop[P]].get(v.getProperty(FIELD))
        }
      }
  }

//  implicit class Rich

  implicit class RichVertex(val v: Vertex) extends AnyVal with RichEntityLike {


    def outgoingEdges: View[Edge] = {
      val edgesIter: EdgeIterator = v.getOutgoingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))

    }

    def incomingEdges: View[Edge] = {
      val edgesIter = v.getIncomingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
    }

  }

  implicit class RichEdge(val v: Edge) extends AnyVal{

    def prop[P: Field](name: String): FieldAccess[P] =
      new FieldAccess[P] {

        override def set(p: P): Unit = {
          val FIELD = v.getRaphtory.getEdgeFieldId(name)
          implicitly[Field[P]].set(v.getField(FIELD), p)
        }

        override def get: P = {
          val FIELD = v.getRaphtory.getEdgeFieldId(name)
          implicitly[Field[P]].get(v.getField(FIELD))
        }
      }

  }
}
