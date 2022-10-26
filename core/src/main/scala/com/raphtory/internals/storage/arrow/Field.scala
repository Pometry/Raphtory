package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.model.Entity

import scala.annotation.implicitNotFound

@implicitNotFound("Could not find arrow property accessor for C[${P}]")
sealed trait Field[P] {
  def set(efa: EntityFieldAccessor, v: P): Unit
  def get(efa: EntityFieldAccessor): Option[P]
}

object Field {

  implicit val strField: Field[String] = new Field[String] {

    override def set(efa: EntityFieldAccessor, p: String): Unit =
      efa.set(p)

    override def get(efa: EntityFieldAccessor): Option[String] = {
      Option(efa.getString).map(_.toString).filter(_.nonEmpty)
    }
  }

  def runtime[T]: Field[T] =
    new Field[T] {

      override def set(efa: EntityFieldAccessor, v: T): Unit =
        efa match {
          case _: EntityFieldAccessor.StringFieldAccessor => efa.set(v.asInstanceOf[String])
        }

      override def get(efa: EntityFieldAccessor): Option[T] =
        efa match {
          case _: EntityFieldAccessor.StringFieldAccessor =>
            if (efa.isSet) Some(efa.getString.toString.asInstanceOf[T])
            else None
        }
    }
}

trait FieldAccess[P] {
  def set(p: P): Unit
  def get: Option[P]
}
