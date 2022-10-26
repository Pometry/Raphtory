package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Entity

import scala.annotation.implicitNotFound
import scala.collection.View

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Prop[P] {
  def set(acc: VersionedEntityPropertyAccessor, v: P, at: Long): Unit
  def get(acc: VersionedEntityPropertyAccessor): Option[P]

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

      override def get(acc: VersionedEntityPropertyAccessor): Option[V] =
        if (acc.isSet) Some(getF(acc))
        else
          getF(acc) match { //FIXME this is a bug with
            case text: String if text.nonEmpty => Some(text.asInstanceOf[V])
            case _                             => None
          }
    }

  /**
    * Use this to conjure a prop at runtime
    * @tparam T
    * @return
    */
  def runtime[T](e: Entity): Prop[T] =
    new Prop[T] {

      override def set(acc: VersionedEntityPropertyAccessor, v: T, at: Long): Unit = ???

      override def get(acc: VersionedEntityPropertyAccessor): Option[T] = {
        val out = acc match {
          case accessor: VersionedEntityPropertyAccessor.IntPropertyAccessor     =>
            if (accessor.isSet) Some(accessor.getInt) else None
          case accessor: VersionedEntityPropertyAccessor.LongPropertyAccessor    =>
            if (accessor.isSet) Some(accessor.getLong) else None
          case accessor: VersionedEntityPropertyAccessor.StringPropertyAccessor  =>
            if (accessor.isSet) Some(accessor.getString.toString) else None
          case accessor: VersionedEntityPropertyAccessor.FloatPropertyAccessor   =>
            if (accessor.isSet) Some(accessor.getFloat) else None
          case accessor: VersionedEntityPropertyAccessor.DoublePropertyAccessor  =>
            if (accessor.isSet) Some(accessor.getDouble) else None
          case accessor: VersionedEntityPropertyAccessor.BooleanPropertyAccessor =>
            if (accessor.isSet) Some(accessor.getBoolean) else None
        }
        out.asInstanceOf[Option[T]]
      }

    }
}

trait PropAccess[P] {
  def set(p: P, at: Long): Unit
  def get: Option[P]

  def list: View[(P, Long)]
}
