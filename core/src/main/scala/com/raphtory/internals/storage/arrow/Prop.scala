package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Entity

import scala.annotation.implicitNotFound
import scala.collection.View

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Prop[P] {
  def set(acc: VersionedEntityPropertyAccessor, v: P, at: Long): Unit
  def get(acc: VersionedEntityPropertyAccessor): P

}

object Prop {

  implicit val intProp: Prop[Int]   = makeProp[Int]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getInt)
  implicit val longProp: Prop[Long] = makeProp[Long]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getLong)

  implicit val doubleProp: Prop[Double] =
    makeProp[Double]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getDouble)

  implicit val floatProp: Prop[Float] =
    makeProp[Float]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getFloat)

  implicit val booleanProp: Prop[Boolean] =
    makeProp[Boolean]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getBoolean)

  implicit val stringProp: Prop[String] =
    makeProp[String]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getString.toString)

  private def makeProp[V](
      setF: (VersionedEntityPropertyAccessor, V, Long) => Unit
  )(getF: VersionedEntityPropertyAccessor => V) =
    new Prop[V] {
      override def set(acc: VersionedEntityPropertyAccessor, v: V, at: Long): Unit = setF(acc, v, at)

      override def get(acc: VersionedEntityPropertyAccessor): V = getF(acc)
    }

  /**
    * Use this to conjure a prop at runtime
    * @tparam T
    * @return
    */
  def runtime[T](e: Entity): Prop[T] =
    new Prop[T] {

      override def set(acc: VersionedEntityPropertyAccessor, v: T, at: Long): Unit = {}

      override def get(acc: VersionedEntityPropertyAccessor): T = {
        val out = acc match {
          case accessor: VersionedEntityPropertyAccessor.IntPropertyAccessor     =>
            accessor.getInt
          case accessor: VersionedEntityPropertyAccessor.LongPropertyAccessor    =>
            accessor.getLong
          case accessor: VersionedEntityPropertyAccessor.StringPropertyAccessor  =>
            accessor.getString.toString
          case accessor: VersionedEntityPropertyAccessor.FloatPropertyAccessor   =>
            accessor.getFloat
          case accessor: VersionedEntityPropertyAccessor.DoublePropertyAccessor  =>
            accessor.getDouble
          case accessor: VersionedEntityPropertyAccessor.BooleanPropertyAccessor =>
            accessor.getBoolean
        }
        out.asInstanceOf[T]
      }

    }
}

trait PropAccess[P] {
  def set(p: P, at: Long): Unit
  def get: P

  def list: View[(P, Long)]
}
