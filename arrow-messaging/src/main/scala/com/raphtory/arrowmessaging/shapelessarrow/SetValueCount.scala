package com.raphtory.arrowmessaging.shapelessarrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import shapeless.{::, Generic, HList, HNil}

trait SetValueCount[T] {
  def setValueCount(vector: T, rows: Int): Unit
}

object SetValueCount {
  def apply[T](implicit derivative: SetValueCount[T]): SetValueCount[T] =
    derivative

  def instance[T](func: (T, Int) => Unit): SetValueCount[T] =
    new SetValueCount[T] {
      override def setValueCount(vector: T, rows: Int): Unit =
        func(vector, rows)
    }

  implicit val intVectorSetValueCount: SetValueCount[IntVector] =
    SetValueCount.instance[IntVector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val float4VectorSetValueCount: SetValueCount[Float4Vector] =
    SetValueCount.instance[Float4Vector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val float8VectorSetValueCount: SetValueCount[Float8Vector] =
    SetValueCount.instance[Float8Vector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val bigIntVectorSetValueCount: SetValueCount[BigIntVector] =
    SetValueCount.instance[BigIntVector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val varCharVectorSetValueCount: SetValueCount[VarCharVector] =
    SetValueCount.instance[VarCharVector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val bitVectorSetValueCount: SetValueCount[BitVector] =
    SetValueCount.instance[BitVector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit val listVectorSetValueCount: SetValueCount[ListVector] =
    SetValueCount.instance[ListVector] { case (vector, rows) => vector.setValueCount(rows) }

  implicit def hNilSetValueCount: SetValueCount[HNil] =
    SetValueCount.instance[HNil] { case (vector, rows) => () }

  implicit def hListSetValueCount[H, T <: HList](
                                                  implicit
                                                  hSetValueCount: SetValueCount[H],
                                                  tSetValueCount: SetValueCount[T]
                                                ): SetValueCount[H :: T] =
    SetValueCount.instance { case (h :: t, rows) =>
      hSetValueCount.setValueCount(h, rows)
      tSetValueCount.setValueCount(t, rows)
    }

  implicit def genericClose[A, R](
                                   implicit
                                   gen: Generic.Aux[A, R],
                                   derivative: SetValueCount[R]
                                 ): SetValueCount[A] = {
    SetValueCount.instance { case (adt, rows) =>
      derivative.setValueCount(gen.to(adt), rows)
    }
  }
}
