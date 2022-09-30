package com.raphtory.arrowmessaging.shapelessarrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import shapeless.{::, Generic, HList, HNil}

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

trait SetSafe[T, R] {
  def setSafe(vector: T, row: Int, value: R)(c: ConcurrentHashMap[ListVector, UnionListWriter]): Unit
}

object SetSafe {
  def apply[T, R](implicit derivative: SetSafe[T, R]): SetSafe[T, R] =
    derivative

  def instance[T, R](func: (T, Int, R, ConcurrentHashMap[ListVector, UnionListWriter]) => Unit): SetSafe[T, R] =
    new SetSafe[T, R] {
      override def setSafe(vector: T, row: Int, value: R)(c: ConcurrentHashMap[ListVector, UnionListWriter]): Unit =
        func(vector, row, value, c)
    }

  implicit class FetchWriter[A <: ListVector](value: A) {
    def getListVectorWriter(vector: ListVector, c: ConcurrentHashMap[ListVector, UnionListWriter]): UnionListWriter = {
      c.putIfAbsent(vector, value.getWriter)
      c.get(vector)
    }
  }

  implicit val intVectorSetSafe: SetSafe[IntVector, Int] =
    SetSafe.instance[IntVector, Int] { case (vector, row, value, _) => vector.setSafe(row, value) }

  implicit val float4VectorSetSafe: SetSafe[Float4Vector, Float] =
    SetSafe.instance[Float4Vector, Float] { case (vector, row, value, _) => vector.setSafe(row, value) }

  implicit val float8VectorSetSafe: SetSafe[Float8Vector, Double] =
    SetSafe.instance[Float8Vector, Double] { case (vector, row, value, _) => vector.setSafe(row, value) }

  implicit val bigIntVectorSetSafe: SetSafe[BigIntVector, Long] =
    SetSafe.instance[BigIntVector, Long] { case (vector, row, value, _) => vector.setSafe(row, value) }

  implicit val varCharVectorSetSafe: SetSafe[VarCharVector, String] =
    SetSafe.instance[VarCharVector, String] { case (vector, row, value, _) => vector.setSafe(row, value.getBytes) }

  implicit val bitVectorSetSafe: SetSafe[BitVector, Boolean] =
    SetSafe.instance[BitVector, Boolean] { case (vector, row, value, _) => vector.setSafe(row, if (value) 1 else 0) }

  implicit val charVectorSetSafe: SetSafe[VarCharVector, Char] =
    SetSafe.instance[VarCharVector, Char] { case (vector, row, value, _) => vector.setSafe(row, Array[Byte](value.asInstanceOf[Byte])) }

  implicit val listVectorSetSafeSetInt: SetSafe[ListVector, Set[Int]] =
    SetSafe.instance[ListVector, Set[Int]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)
      value.foreach(e => writer.writeInt(e))
      writer.endList()
    }

  implicit val listVectorSetSafeSetStr: SetSafe[ListVector, Set[String]] =
    SetSafe.instance[ListVector, Set[String]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)

      value.foreach { str =>
        val bytes = str.getBytes(StandardCharsets.UTF_8)
        val length = bytes.size

        val buffer = vector.getAllocator.buffer(length) // TODO Fix this to better buffer allocation strategy !!
        buffer.setBytes(0, bytes)
        writer.writeVarChar(0, length, buffer)
        buffer.clear()
      }

      writer.endList()
    }

  implicit val listVectorSetSafeListLong: SetSafe[ListVector, List[Long]] =
    SetSafe.instance[ListVector, List[Long]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)
      value.foreach(e => writer.writeBigInt(e))
      writer.endList()
    }

  implicit val listVectorSetSafeListInt: SetSafe[ListVector, List[Int]] =
    SetSafe.instance[ListVector, List[Int]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)
      value.foreach(e => writer.writeInt(e))
      writer.endList()
    }

  implicit val listVectorSetSafeListDouble: SetSafe[ListVector, List[Double]] =
    SetSafe.instance[ListVector, List[Double]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)
      value.foreach(e => writer.writeFloat8(e))
      writer.endList()
    }

  implicit val listVectorSetSafeListFloat: SetSafe[ListVector, List[Float]] =
    SetSafe.instance[ListVector, List[Float]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)
      value.foreach(e => writer.writeFloat4(e))
      writer.endList()
    }

  implicit val listVectorSetSafeListStr: SetSafe[ListVector, List[String]] =
    SetSafe.instance[ListVector, List[String]] { case (vector, row, value, c) =>
      val writer = vector.getListVectorWriter(vector, c)
      writer.startList()
      writer.setPosition(row)

      value.foreach { str =>
        val bytes = str.getBytes(StandardCharsets.UTF_8)
        val length = bytes.size

        val buffer = vector.getAllocator.buffer(length) // TODO Fix this to better buffer allocation strategy !!
        buffer.setBytes(0, bytes)
        writer.writeVarChar(0, length, buffer)
        buffer.clear()
      }

      writer.endList()
    }

  implicit def hNilSetSafe: SetSafe[HNil, HNil] =
    SetSafe.instance[HNil, HNil] { case (vector, row, value, _) => () }

  implicit def hListSetSafe[H, T <: HList, P, Q <: HList](
                                                           implicit
                                                           hSetSafe: SetSafe[H, P],
                                                           tSetSafe: SetSafe[T, Q],
                                                         ): SetSafe[H :: T, P :: Q] =
    SetSafe.instance { case (h :: t, row, p :: q, c) =>
      hSetSafe.setSafe(h, row, p)(c)
      tSetSafe.setSafe(t, row, q)(c)
    }

  implicit def genericSetSafe[A, ARepr <: HList, B, BRepr <: HList](
                                                                     implicit
                                                                     genA: Generic.Aux[A, ARepr],
                                                                     genB: Generic.Aux[B, BRepr],
                                                                     derivative: SetSafe[ARepr, BRepr],
                                                                   ): SetSafe[A, B] = {
    SetSafe.instance { case (vectors, row, values, c) =>
      derivative.setSafe(genA.to(vectors), row, genB.to(values))(c)
    }
  }

}
