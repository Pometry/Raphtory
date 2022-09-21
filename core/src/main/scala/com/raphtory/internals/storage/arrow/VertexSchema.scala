package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.NonversionedField
import com.raphtory.arrowcore.implementation.VersionedProperty

trait VertexSchema[T] {
  def nonVersionedVertexProps(name: Option[String]): Iterable[NonversionedField]
  def versionedVertexProps(name: Option[String]): Iterable[VersionedProperty]
}

object VertexSchema {
  import magnolia1._

  import language.experimental.macros
  import scala.reflect.ClassTag

  type Typeclass[T] = VertexSchema[T]

  def join[T](ctx: CaseClass[VertexSchema, T]): VertexSchema[T] =
    new Typeclass[T] {

      override def nonVersionedVertexProps(name: Option[String]): Iterable[NonversionedField] =
        ctx.parameters
          .filter(_.annotations.exists(_.isInstanceOf[immutable]))
          .flatMap { p =>
            p.typeclass.nonVersionedVertexProps(Some(p.label))
          }

      override def versionedVertexProps(name: Option[String]): Iterable[VersionedProperty] =
        ctx.parameters
          .filterNot(_.annotations.exists(_.isInstanceOf[immutable]))
          .flatMap { p =>
            p.typeclass.versionedVertexProps(Some(p.label))
          }

    }

  implicit def gen[T]: VertexSchema[T] = macro Magnolia.gen[T]

  implicit val longArrowSchema: Typeclass[Long]       = baseTypeClass[Long, Long]
  implicit val booleanArrowSchema: Typeclass[Boolean] = baseTypeClass[Boolean, Boolean]
  implicit val stringArrowSchema: Typeclass[String]   = baseTypeClass[String, java.lang.StringBuilder]
  implicit val floatArrowSchema: Typeclass[Float]     = baseTypeClass[Float, Float]
  implicit val doubleArrowSchema: Typeclass[Double]   = baseTypeClass[Double, Double]
  implicit val intArrowSchema: Typeclass[Int]         = baseTypeClass[Int, Int]

  def baseTypeClass[T, Arr](implicit ct: ClassTag[Arr]): Typeclass[T] =
    new Typeclass[T] {

      override def nonVersionedVertexProps(name: Option[String]): Iterable[NonversionedField] =
        List(new NonversionedField(name.get, ct.runtimeClass))

      override def versionedVertexProps(name: Option[String]): Iterable[VersionedProperty] =
        List(new VersionedProperty(name.get, ct.runtimeClass))
    }
}
