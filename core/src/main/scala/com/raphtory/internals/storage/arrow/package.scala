package com.raphtory.internals.storage

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.arrowcore.implementation.EdgeHistoryIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.NonversionedEnumField
import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
import com.raphtory.arrowcore.implementation.VertexHistoryIterator
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Vertex
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

        override def get: Option[P] = {
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

    def history(start: Long, end: Long): View[HistoricEvent] =
      View.fromIteratorProvider { () =>
        val vhi: VertexHistoryIterator.WindowedVertexHistoryIterator =
          v.getRaphtory.getNewVertexHistoryIterator(v.getLocalId, start, end)
        new ArrowPartition.VertexHistoryIterator(vhi)
      }

    def outgoingEdges: View[Edge] = {
      val edgesIter: EdgeIterator = v.getOutgoingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
    }

    def outgoingEdgesIter: Iterator[Edge] = {
      val edgesIter: EdgeIterator = v.getOutgoingEdges
      new ArrowPartition.EdgesIterator(edgesIter)
    }

    def outgoingEdges(start: Long, end: Long): View[Edge] =
      View.fromIteratorProvider { () =>
        val iter: VertexIterator.WindowedVertexIterator = windowIter(start, end)
        new ArrowPartition.EdgesIterator(iter.getOutgoingEdges)
      }

    def outDegree(start: Long, end: Long): Int = {
      v.nOutgoingEdges()
//      val i = v.getRaphtory.getNewAllVerticesIterator
//      i.reset(v.getLocalId)
//      i.getNOutgoingEdges
      outgoingEdges(start, end).size
    }

    def inDegree(start: Long, end: Long): Int = {
//      val i = v.getRaphtory.getNewAllVerticesIterator
//      i.reset(v.getLocalId)
//      i.getNIncomingEdges

      incomingEdges(start, end).size
    }

    private def windowIter(start: Long, end: Long) = {
      val iter = v.getRaphtory.getNewWindowedVertexIterator(start, end)
      iter.reset(v.getLocalId)
      iter.next()
      val v0   = iter.getVertex
      iter
    }

    def incomingEdges(start: Long, end: Long): View[Edge] =
      View.fromIteratorProvider { () =>
        val iter: VertexIterator.WindowedVertexIterator = windowIter(start, end)
        new ArrowPartition.EdgesIterator(iter.getIncomingEdges)
      }

    def incomingEdges: View[Edge] =
      View.fromIteratorProvider { () =>
        val edgesIter = v.getIncomingEdges
        new ArrowPartition.EdgesIterator(edgesIter)
      }

    def outgoingEdges(dst: Long, isDstGlobal: Boolean): View[Edge] =
      View.fromIteratorProvider { () =>
        new ArrowPartition.MatchingEdgesIterator(v.findAllOutgoingEdges(dst, isDstGlobal))
      }
  }

  implicit class RichEdge(val v: Edge) extends AnyVal {

    def field[P: Field](name: String): FieldAccess[P] =
      new FieldAccess[P] {

        override def set(p: P): Unit = {
          val FIELD = v.getRaphtory.getEdgeFieldId(name)
          implicitly[Field[P]].set(v.getField(FIELD), p)
        }

        override def get: Option[P] = {
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
            val FIELD    = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
            val iterator = v.getPropertyHistory(FIELD)

            View.fromIteratorProvider(() => new PropertyIterator(iterator)).flatten
          }
          catch {
            case _: IllegalArgumentException =>
              View.empty
            case _: IllegalStateException    => // FIXME: this should not be the case!
              View.empty
          }
      }

    def history(start: Long, end: Long): View[HistoricEvent] =
      View.fromIteratorProvider { () =>
        val ehi = v.getRaphtory.getNewEdgeHistoryIterator(v.getLocalId, start, end)
        new ArrowPartition.EdgeHistoryIterator(ehi)
      }

  }

}
