package com.raphtory.internals.storage

import com.raphtory.arrowcore.implementation.{EdgeIterator, EntityFieldAccessor, NonversionedEnumField, VersionedEntityPropertyAccessor}
import com.raphtory.arrowcore.model.{Edge, Vertex}
import com.raphtory.internals.storage.arrow.ArrowPartition.PropertyIterator

import scala.collection.View
package object arrow {

  implicit class RichVertex(val v: Vertex) extends AnyVal {

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

        override def get: Option[P] = {
          val FIELD = v.getRaphtory.getVertexPropertyId(name.toLowerCase())
          implicitly[Prop[P]].get(v.getProperty(FIELD))
        }

        override def list: View[(P, Long)] =
          try {
            val FIELD = v.getRaphtory.getVertexPropertyId(name.toLowerCase())
            View.fromIteratorProvider(() => new PropertyIterator(v.getPropertyHistory(FIELD))).flatten
          }
          catch {
            case _: IllegalArgumentException =>
              View.empty
          }
      }

    def outgoingEdges: View[Edge] = {
      val edgesIter: EdgeIterator = v.getOutgoingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
    }

    def outgoingEdges(start: Long, end: Long): View[Edge] =
      View.fromIteratorProvider { () =>
        val iter = v.getRaphtory.getNewWindowedVertexIterator(start, end)
        iter.reset(v.getLocalId)
        new ArrowPartition.EdgesIterator(iter.getOutgoingEdges)
      }

    def incomingEdges(start: Long, end: Long): View[Edge] =
      View.fromIteratorProvider { () =>
        val iter = v.getRaphtory.getNewWindowedVertexIterator(start, end)
        iter.reset(v.getLocalId)
        new ArrowPartition.EdgesIterator(iter.getIncomingEdges)
      }

    def incomingEdges: View[Edge] = {
      val edgesIter = v.getIncomingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
    }

    def outgoingEdge(dst: Long): Option[Edge] = {
      val iter = v.findAllOutgoingEdges(dst)
      if (iter.hasNext)
        Some(iter.getEdge)
      else
        None
    }

  }

  implicit class RichEdge(val v: Edge) extends AnyVal {

    def field[P: Field](name: String): FieldAccess[P] =
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

    def prop[P: Prop](name: String): PropAccess[P] =
      new PropAccess[P] {

        override def set(p: P, at: Long): Unit = {
          val FIELD = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
          implicitly[Prop[P]].set(v.getProperty(FIELD), p, at)
        }

        override def get: Option[P] = {
          val FIELD = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
          implicitly[Prop[P]].get(v.getProperty(FIELD))
        }

        override def list: View[(P, Long)] =
          try {
            val FIELD = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
            val iterator = v.getPropertyHistory(FIELD)

            View.fromIteratorProvider(() => new PropertyIterator(iterator)).flatten
          }
          catch {
            case _: IllegalArgumentException =>
              View.empty
            case _: IllegalStateException => // FIXME: this should not be the case!
              View.empty
          }
      }
  }

}
