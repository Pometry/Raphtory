package com.raphtory.arrowmessaging.shapelessarrow

import com.raphtory.arrowmessaging.model.ArrowFlightMessage
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.objenesis.ObjenesisStd
import shapeless.::
import shapeless.Generic
import shapeless.HList
import shapeless.HNil

import java.lang.IllegalStateException
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.reflect.ClassTag

trait Get[T, R] {
  import Get._

  def get(vector: T, row: Int, value: R): R

  def invokeGet(vector: T, row: Int)(implicit ct: ClassTag[R]): R = {
    def newDefault[A](implicit t: ClassTag[A]): A = {
      val clazz = t.runtimeClass
      val name  = clazz.getName

      if (!cache.containsKey(name)) {
        val instantiator = objenesis.getInstantiatorOf(clazz)
        val newInstance  = instantiator.newInstance().asInstanceOf[ArrowFlightMessage].withDefaults()
        cache.putIfAbsent(name, newInstance)
      }
      cache.get(name).asInstanceOf[A]
    }

    try {
      get(vector, row, newDefault[R])
    } catch {
      case e: NegativeArraySizeException => throw new IllegalStateException(e)
    }
  }
}

object Get {

  val cache     = new ConcurrentHashMap[String, ArrowFlightMessage]()
  val objenesis = new ObjenesisStd()

  def apply[T, R](implicit derivative: Get[T, R]): Get[T, R] =
    derivative

  def instance[T, R](func: (T, Int, R) => R): Get[T, R] =
    new Get[T, R] {

      override def get(vector: T, row: Int, value: R): R =
        func(vector, row, value)
    }

  implicit val intVectorGet: Get[IntVector, Int]            =
    Get.instance[IntVector, Int] { case (vector, row, value) => vector.get(row) }

  implicit val float4VectorGet: Get[Float4Vector, Float]    =
    Get.instance[Float4Vector, Float] { case (vector, row, value) => vector.get(row) }

  implicit val float8VectorGet: Get[Float8Vector, Double]   =
    Get.instance[Float8Vector, Double] { case (vector, row, value) => vector.get(row) }

  implicit val bigIntVectorGet: Get[BigIntVector, Long]     =
    Get.instance[BigIntVector, Long] { case (vector, row, value) => vector.get(row) }

  implicit val varCharVectorGet: Get[VarCharVector, String] =
    Get.instance[VarCharVector, String] {
      case (vector, row, value) =>
        new String(vector.get(row), StandardCharsets.UTF_8)
    }

  implicit val bitVectorGet: Get[BitVector, Boolean] =
    Get.instance[BitVector, Boolean] {
      case (vector, row, value) =>
        if (vector.get(row) == 1) true else false
    }

  implicit val charVectorGet: Get[VarCharVector, Char] =
    Get.instance[VarCharVector, Char] {
      case (vector, row, value) =>
        vector.get(row)(0).asInstanceOf[Char]
    }

  implicit val listVectorGetSetInt: Get[ListVector, Set[Int]] =
    Get.instance[ListVector, Set[Int]] {
      case (vector, row, value) =>
        vector.getObject(row).asScala.toSet.asInstanceOf[Set[Int]]
    }

  implicit val listVectorGetSetStr: Get[ListVector, Set[String]] =
    Get.instance[ListVector, Set[String]] {
      case (vector, row, value) =>
        // vector.getObject(row).asScala.toSet.map(e => e.toString)  Doesn't work!!

        val res = mutable.Set.empty[String]
        val itr = vector.getObject(row).iterator()

        while (itr.hasNext) {
          val o = itr.next().asInstanceOf[org.apache.arrow.vector.util.Text]
          res.add(new String(o.getBytes, StandardCharsets.UTF_8))
        }

        res.toSet
    }

  implicit val listVectorGetListLong: Get[ListVector, List[Long]] =
    Get.instance[ListVector, List[Long]] {
      case (vector, row, value) =>
        vector.getObject(row).asScala.toList.asInstanceOf[List[Long]]
    }

  implicit val listVectorGetListInt: Get[ListVector, List[Int]] =
    Get.instance[ListVector, List[Int]] {
      case (vector, row, value) =>
        vector.getObject(row).asScala.toList.asInstanceOf[List[Int]]
    }

  implicit val listVectorGetListDouble: Get[ListVector, List[Double]] =
    Get.instance[ListVector, List[Double]] {
      case (vector, row, value) =>
        vector.getObject(row).asScala.toList.asInstanceOf[List[Double]]
    }

  implicit val listVectorGetListFloat: Get[ListVector, List[Float]] =
    Get.instance[ListVector, List[Float]] {
      case (vector, row, value) =>
        vector.getObject(row).asScala.toList.asInstanceOf[List[Float]]
    }

  implicit val listVectorGetListStr: Get[ListVector, List[String]] =
    Get.instance[ListVector, List[String]] {
      case (vector, row, value) =>
        val res = mutable.ListBuffer.empty[String]
        val itr = vector.getObject(row).iterator()

        while (itr.hasNext) {
          val o = itr.next().asInstanceOf[org.apache.arrow.vector.util.Text]
          res.addOne(new String(o.getBytes, StandardCharsets.UTF_8))
        }

        res.toList
    }

  implicit def hNilGet: Get[HNil, HNil] =
    Get.instance[HNil, HNil] { case (vector, row, value) => HNil }

  implicit def hListGet[H, T <: HList, P, Q <: HList](implicit
      hGet: Get[H, P],
      tGet: Get[T, Q]
  ): Get[H :: T, P :: Q]                =
    Get.instance {
      case (h :: t, row, p :: q) =>
        hGet.get(h, row, p) :: tGet.get(t, row, q)
    }

  implicit def genericGet[A, ARepr <: HList, B, BRepr <: HList](implicit
      genA: Generic.Aux[A, ARepr],
      genB: Generic.Aux[B, BRepr],
      derivative: Get[ARepr, BRepr]
  ): Get[A, B] =
    Get.instance {
      case (vectors, row, values) =>
        genB.from(derivative.get(genA.to(vectors), row, genB.to(values)))
    }
}
