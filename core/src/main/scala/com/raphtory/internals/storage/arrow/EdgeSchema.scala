package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.NonversionedField
import com.raphtory.arrowcore.implementation.VersionedProperty

trait EdgeSchema[T] {

  def nonVersionedEdgeProps(name: Option[String]): Iterable[NonversionedField]
  def versionedEdgeProps(name: Option[String]): Iterable[VersionedProperty]
}

object EdgeSchema {
  import magnolia1._

  import scala.reflect.ClassTag
  import language.experimental.macros

  type Typeclass[T] = EdgeSchema[T]

  def join[T](ctx: CaseClass[EdgeSchema, T]): EdgeSchema[T] =
    new Typeclass[T] {

      override def nonVersionedEdgeProps(name: Option[String]): Iterable[NonversionedField] =
        ctx.parameters
          .filterNot(_.annotations.exists(_.isInstanceOf[versioned]))
          .map { p =>
            p.typeclass.nonVersionedEdgeProps(Some(p.label))
          }
          .reduce(_ ++ _)
          .toList

      override def versionedEdgeProps(name: Option[String]): Iterable[VersionedProperty] =
        ctx.parameters
          .filter(_.annotations.exists(_.isInstanceOf[versioned]))
          .map { p =>
            p.typeclass.versionedEdgeProps(Some(p.label))
          }
          .reduce(_ ++ _)
          .toList

    }

  implicit def gen[T]: EdgeSchema[T] = macro Magnolia.gen[T]

  implicit val longArrowSchema: Typeclass[Long]       = baseTypeClass[Long, Long]
  implicit val booleanArrowSchema: Typeclass[Boolean] = baseTypeClass[Boolean, Boolean]
  implicit val stringArrowSchema: Typeclass[String]   = baseTypeClass[String, java.lang.StringBuilder]

  def baseTypeClass[T, Arr](implicit ct: ClassTag[Arr]): Typeclass[T] =
    new Typeclass[T] {

      override def nonVersionedEdgeProps(name: Option[String]): Iterable[NonversionedField] =
        List(new NonversionedField(name.get, ct.runtimeClass))

      override def versionedEdgeProps(name: Option[String]): Iterable[VersionedProperty] =
        List(new VersionedProperty(name.get, ct.runtimeClass))

    }
}
