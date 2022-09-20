package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor

import scala.annotation.implicitNotFound

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Prop[P] {
  def set(acc: VersionedEntityPropertyAccessor, v: P, at: Long): Unit
  def get(acc: VersionedEntityPropertyAccessor): P
}

object Prop {

  implicit val strProp: Prop[String] = new Prop[String] {
    override def set(acc: VersionedEntityPropertyAccessor, v: String, at: Long): Unit = {
      acc.setHistory(false, at).set(v)
    }

    override def get(acc: VersionedEntityPropertyAccessor): String = {
     acc.getString.toString
    }

  }

  implicit val longProp: Prop[Long] = new Prop[Long]{
    override def set(acc: VersionedEntityPropertyAccessor, v: Long, at: Long): Unit = {
      acc.setHistory(false, at).set(v)
    }

    override def get(acc: VersionedEntityPropertyAccessor): Long = {
      acc.getLong
    }
  }
}

trait PropAccess[P] {
  def set(p: P, at:Long): Unit
  def get: P
}
