package com.raphtory.internals.management

import munit.FunSuite
import PythonInterop._

import scala.reflect.runtime.universe._
import scala.reflect
import com.raphtory.api.input.GraphBuilder

import scala.reflect.runtime.currentMirror

object TestMethods {
  def varArgs(test: Int*): Unit = {}
  val varargsType = {
    val objType = currentMirror.classSymbol(TestMethods.getClass).toType.dealias
    val m = objType.members.find(p => p.isMethod && p.asMethod.name.toString == "varArgs").get
    val param = m.asMethod.paramLists(0).head
    param.info
  }
}

class PythonInteropTypeReprTest[T] extends FunSuite {

  test("test function type check") {
    assert(function1Type(typeOf[(Int) => Long ]))
  }

  test("Sequence is not function") {
    assert(!(function1Type(typeOf[Seq[Any]])))
  }

  test("PythonFunction is function") {
    assert(function1Type(typeOf[PythonFunction1[Object, Any]]))
  }

  test("GraphBuilder is not Function2") {
    assert(!function2Type(typeOf[GraphBuilder[String]]))
  }

  test("normal Function2") {
    assert(function2Type(typeOf[(Any, Any) => Any]))
  }

  test("PythonFunction2 should also pass") {
    assert(function2Type(typeOf[PythonFunction2[AnyRef, AnyRef, Any]]))
  }

  test("lists are lists") {
    assert(scalaListType(typeOf[List[Int]]))
  }

  test("immutable Seq is also List for now") {
    assert(scalaListType(typeOf[Seq[Int]]))
  }

  test("Vector is not List") {
    assert(!scalaListType(typeOf[Vector[Any]]))
  }
  test("other Seq is not list") {
    assert(!scalaListType(typeOf[collection.Seq[Int]]))
  }

  test("collection.Seq is sequence") {
    assert(sequenceType(typeOf[collection.Seq[Int]]))
  }

  test("Iterable is not sequence") {
    assert(!sequenceType(typeOf[Iterable[Int]]))
  }

  test("Iterable is iterable") {
    assert(iterableType(typeOf[Iterable[Int]]))
  }

  test("Seq is not iterable") {
    assert(!iterableType(typeOf[Seq[Int]]))
  }

  test("IterableOnce is Iterator") {
    assert(iteratorType(typeOf[IterableOnce[Any]]))
  }

  test("Iterator is Iterator") {
    assert(iteratorType(typeOf[Iterator[Any]]))
  }

  test("Iterable is not Iterator") {
    assert(!iteratorType(typeOf[Iterable[Any]]))
  }

  test("Iterable is Iterable") {
    assert(iterableType(typeOf[Iterable[Any]]))
  }

  test("iterator is not Iterable") {
    assert(!iterableType(typeOf[Iterator[Any]]))
  }

  test("IterableOnce is Iterable") {
    assert(iterableType(typeOf[IterableOnce[Any]]))
  }

  test("Bytes are not Array") {
    assert(!arrayType(typeOf[Array[Byte]]))
  }

  test("Arrays are Array") {
    assert(arrayType(typeOf[Array[Any]]))
  }

  test("Bytes are bytes") {
    assert(bytesType(typeOf[Array[Byte]]))
  }

  test("Other Arrays are not bytes") {
    assert(!bytesType(typeOf[Array[Any]]))
  }

  test("Python list type matches IterableOnce") {
    assert(pyListType(typeOf[IterableOnce[Any]]))
  }

  test("Python list type matches Array") {
    assert(pyListType(typeOf[Array[Any]]))
  }

  test("varargs is not List") {
    assert(!scalaListType(TestMethods.varargsType))
  }

  test("Test varargs type matcher") {
    assert(varargsType(TestMethods.varargsType))
  }

  test("List is not varargs") {
    assert(!varargsType(typeOf[Seq[Any]]))
  }

  test("Test actual type is not generic type label") {
    assert(!genericType(typeOf[Any]))
  }

  test("Test generic type label") {
    assert(genericType(weakTypeOf[T]))
  }

}
