package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor

import scala.annotation.implicitNotFound

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

  implicit val stringProp: Prop[String]   =
    makeProp[String]((acc, v, at) => acc.setHistory(false, at).set(v))(_.getString.toString)

  private def makeProp[V](
      setF: (VersionedEntityPropertyAccessor, V, Long) => Unit
  )(getF: VersionedEntityPropertyAccessor => V) =
    new Prop[V] {
      override def set(acc: VersionedEntityPropertyAccessor, v: V, at: Long): Unit = setF(acc, v, at)

      override def get(acc: VersionedEntityPropertyAccessor): V = getF(acc)
    }
}

trait PropAccess[P] {
  def set(p: P, at: Long): Unit
  def get: P
}
