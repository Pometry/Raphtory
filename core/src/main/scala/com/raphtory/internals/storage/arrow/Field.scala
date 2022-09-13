package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.EntityFieldAccessor

import scala.annotation.implicitNotFound

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Field[P] {
  def set(efa: EntityFieldAccessor, v: P): Unit
  def get(efa: EntityFieldAccessor): P
}

object Field {

  implicit val strField: Field[String] = new Field[String] {

    override def set(efa: EntityFieldAccessor, p: String): Unit =
      efa.set(p)

    override def get(efa: EntityFieldAccessor): String =
      efa.getString.toString
  }
}

trait FieldAccess[P] {
  def set(p: P): Unit
  def get: P
}