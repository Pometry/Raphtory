package com.raphtory.internals.storage

import com.raphtory.arrowcore.implementation.ArrowPropertyIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.NonversionedEnumField
import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
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

        override def list: View[(P, Long)] =
          try {
            val FIELD = v.getRaphtory.getVertexPropertyId(name.toLowerCase())
            View.fromIteratorProvider(() => new PropertyIterator(v.getPropertyHistory(FIELD)))
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

    def incomingEdges: View[Edge] = {
      val edgesIter = v.getIncomingEdges
      View.from(new ArrowPartition.EdgesIterator(edgesIter))
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

        override def get: P = {
          val FIELD = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
          implicitly[Prop[P]].get(v.getProperty(FIELD))
        }

        override def list: View[(P, Long)] =
          try {
            val FIELD = v.getRaphtory.getEdgePropertyId(name.toLowerCase())
            View.fromIteratorProvider(() => new PropertyIterator(v.getPropertyHistory(FIELD)))
          }
          catch {
            case _: IllegalArgumentException =>
              View.empty
          }
      }
  }

  implicit class RichFieldAccessor(val acc: EntityFieldAccessor) extends AnyVal {

    def getAny: Any =
      acc match {
        case accessor: EntityFieldAccessor.IntFieldAccessor          => accessor.getInt
        case accessor: EntityFieldAccessor.ByteFieldAccessor         => accessor.getByte
        case accessor: EntityFieldAccessor.LongFieldAccessor         => accessor.getLong
        case accessor: EntityFieldAccessor.FloatFieldAccessor        => accessor.getFloat
        case accessor: EntityFieldAccessor.ShortFieldAccessor        => accessor.getShort
        case accessor: EntityFieldAccessor.DoubleFieldAccessor       => accessor.getDouble
        case accessor: EntityFieldAccessor.StringFieldAccessor       => accessor.getString.toString
        case accessor: EntityFieldAccessor.BooleanFieldAccessor      => accessor.getBoolean
        case accessor: NonversionedEnumField.EnumEntityFieldAccessor => accessor.getEnum
      }
  }

  implicit class RichPropertyAccessor(val acc: VersionedEntityPropertyAccessor) extends AnyVal {

    def getAny: Any =
      acc match {
        case accessor: VersionedEntityPropertyAccessor.IntPropertyAccessor     => accessor.getInt
        case accessor: VersionedEntityPropertyAccessor.LongPropertyAccessor    => accessor.getLong
        case accessor: VersionedEntityPropertyAccessor.FloatPropertyAccessor   => accessor.getFloat
        case accessor: VersionedEntityPropertyAccessor.DoublePropertyAccessor  => accessor.getDouble
        case accessor: VersionedEntityPropertyAccessor.StringPropertyAccessor  => accessor.getString.toString
        case accessor: VersionedEntityPropertyAccessor.BooleanPropertyAccessor => accessor.getBoolean
      }
  }
}
