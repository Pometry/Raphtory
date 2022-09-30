package com.raphtory.arrowmessaging.shapelessarrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import shapeless.{::, Generic, HList, HNil}

trait IsSet[T] {
  def isSet(vector: T, row: Int): List[Int]
}

object IsSet {
  def apply[T](implicit derivative: IsSet[T]): IsSet[T] =
    derivative

  def instance[T](func: (T, Int) => List[Int]): IsSet[T] =
    new IsSet[T] {
      override def isSet(vector: T, row: Int): List[Int] =
        func(vector, row)
    }

  implicit val intVectorIsSet: IsSet[IntVector] =
    IsSet.instance[IntVector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val float4VectorIsSet: IsSet[Float4Vector] =
    IsSet.instance[Float4Vector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val float8VectorIsSet: IsSet[Float8Vector] =
    IsSet.instance[Float8Vector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val bigIntVectorIsSet: IsSet[BigIntVector] =
    IsSet.instance[BigIntVector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val varCharVectorIsSet: IsSet[VarCharVector] =
    IsSet.instance[VarCharVector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val bitVectorIsSet: IsSet[BitVector] =
    IsSet.instance[BitVector] { case (vector, row) => List(vector.isSet(row)) }

  implicit val listVectorIsSet: IsSet[ListVector] =
    IsSet.instance[ListVector] { case (vector, row) => List(vector.isSet(row)) }

  implicit def hNilSetValueCount: IsSet[HNil] =
    IsSet.instance[HNil] { case (vector, row) => Nil }

  implicit def hListSetValueCount[H, T <: HList](
                                                  implicit
                                                  hIsSet: IsSet[H],
                                                  tIsSet: IsSet[T]
                                                ): IsSet[H :: T] =
    IsSet.instance { case (h :: t, row) =>
      hIsSet.isSet(h, row) ++ tIsSet.isSet(t, row)
    }

  implicit def genericIsSet[A, R](
                                   implicit
                                   gen: Generic.Aux[A, R],
                                   derivative: IsSet[R]
                                 ): IsSet[A] = {
    IsSet.instance { case (adt, row) =>
      derivative.isSet(gen.to(adt), row)
    }
  }

}
