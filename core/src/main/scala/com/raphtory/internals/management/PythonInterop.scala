package com.raphtory.internals.management

import cats.Id
import cats.syntax.all._

import java.lang.reflect.{Array => JArray}
import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.graph.GraphBuilder
import com.raphtory.internals.management.python.EmbeddedPython
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe
import scala.util.Random

/** Scala-side methods for interfacing with Python */
object PythonInterop {
  val logger: WrappedLogger = new WrappedLogger(Logger(LoggerFactory.getLogger(this.getClass)))

  def repr(obj: Any): String =
    obj match {
      case v: Array[_]    => "[" + v.map(repr).mkString(", ") + "]"
      case v: Iterable[_] => "[" + v.map(repr).mkString(", ") + "]"
      case v: Product     =>
        v.productPrefix + "(" + v.productElementNames
          .zip(v.productIterator)
          .map {
            case (name, value) => s"$name=${repr(value)}"
          }
          .mkString(", ") + ")"
      case v              => v.toString
    }

  /** make assign_id accessible from python */
  def assign_id(s: String): Long =
    GraphBuilder.assignID(s)

  /** convert names from camel to snake case */
  def camel_to_snake(s: String): String =
    PythonEncoder.camelToSnakeCase(s)

  /** used to convert java objects to scala objects when passing through python collections
    * (define more converters as needed)
    */
  def decode[T](obj: Any): T =
    (obj match {
      case obj: java.util.ArrayList[_] => obj.asScala
      case obj                         => obj
    }).asInstanceOf[T]

  /** Look up name of python wrapper based on input type
    * (used to provide specialised wrappers for categories of types rather than specific classes)
    */
  def get_wrapper_str(obj: Any): String =
    obj match {
      case _: collection.Seq[_] => "Sequence"
      case _: Iterable[_]       => "Iterable"
      case _: Iterator[_]       => "Iterator"
      case _: GraphPerspective  => "TemporalGraph"
      case _: Table             => "Table"
      case _: Accumulator[_, _] => "Accumulator"
      case _: Vertex            => "Vertex"
      case _: GraphState        => "GraphState"
      case _                    => "None"
    }

  /** Find the singleton instance of a companion object for a class name
    * (used for constructing objects from python)
    */
  def find_class(name: String): Any = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module        = runtimeMirror.staticModule(name)
    val obj           = runtimeMirror.reflectModule(module)
    obj.instance
  }

  /** Take an iterable of arguments and turn it into a varargs friendly list */
  def make_varargs[T](obj: Iterable[T]): List[T] =
    obj.toList

  def public_methods(obj: Any): Map[String, ArrayBuffer[Method]] = {
    val clazz         = obj.getClass
    val runtimeMirror = universe.runtimeMirror(clazz.getClassLoader)
    val objType       = runtimeMirror.classSymbol(clazz).toType.dealias
    val methods       = objType.members
      .collect {
        case s if s.isTerm && s.isPublic => s.asTerm
      }
      .flatMap {
        case s if s.isMethod     => List(s)
        case s if s.isOverloaded => s.alternatives.filter(_.isMethod)
        case _                   => List.empty
      }
      .map(_.asMethod)
      .filterNot(_.isParamWithDefault) // We will deal with default argument providers later

    val methodMap = mutable.Map.empty[String, ArrayBuffer[Method]]

    methods.foreach { m =>
      val name       = m.name.toString
      val params     = m.paramLists.flatten
      val types      = params.map(p => p.info.toString).toArray
      val implicits  = params.collect { case p if p.isImplicit => camel_to_snake(p.name.toString) }.toArray
      val paramNames = params.filterNot(_.isImplicit).map(p => camel_to_snake(p.name.toString)).toArray
      val defaults   = params.zipWithIndex
        .collect {
          case (p, i) if p.asTerm.isParamWithDefault =>
            (i, name + "$default$" + s"${i + 1}")
        }
        .toMap
        .asJava

      methodMap
        .getOrElseUpdate(name, ArrayBuffer.empty[Method])
        .append(
                Method(
                        name,
                        params.size,
                        paramNames,
                        types,
                        defaults,
                        m.isVarargs,
                        implicits
                )
        )
    }
    methodMap.toMap
  }

  /** Find methods and default values for an object and return in friendly format */
  def methods(obj: Any): util.Map[String, Array[Method]] = {
    logger.trace(s"Scala 'methods' called with $obj")
    val publicMethods = public_methods(obj)
    val javaMethods   = obj.getClass.getMethods.map(m => m.getName).toSet
    val actual        = publicMethods.view
      .filterKeys(m => javaMethods contains m)
      .map { case (key, value) => camel_to_snake(key) -> value.toArray }
      .toMap
      .asJava
    actual
  }
}

/**
  * wrap the logger class as calling the logger directly from python is broken
  */
class WrappedLogger(logger: Logger) {

  def level: Int =
    if (logger.underlying.isTraceEnabled) 5
    else if (logger.underlying.isDebugEnabled) 4
    else if (logger.underlying.isInfoEnabled) 3
    else if (logger.underlying.isWarnEnabled) 2
    else if (logger.underlying.isErrorEnabled) 1
    else 0

  def info(msg: String): Unit = logger.info(msg)

  def debug(msg: String): Unit = logger.debug(msg)

  def trace(msg: String): Unit = logger.trace(msg)

  def warn(msg: String): Unit = logger.warn(msg)

  def error(msg: String): Unit = logger.error(msg)
}

/** Representation of a method */
case class Method(
    name: String,
    n: Int,
    parameters: Array[String],
    types: Array[String],
    defaults: java.util.Map[Int, String],
    varargs: Boolean,
    implicits: Array[String]
) {
  def has_defaults: Boolean = !defaults.isEmpty
}

/**
  * Reference of object inside python
  * @param name
  * name of variable inside the python context
  */
case class PyRef(name: String)

/**
  * Wrapper for a python function that uses the python interpreter to turn it into a scala function
  *
  * (Need to provide specialised implementations for all different numbers of arguments)
  */
trait PythonFunction {
  protected val pickleBytes: Array[Byte]
  protected val eval_name = s"_${Random.alphanumeric.take(32).mkString}"

  private def py: EmbeddedPython[Id] = EmbeddedPython.global

  private def initialize(py: EmbeddedPython[Id]) = {
    try py.run(eval_name)
    catch {
      case e: Throwable =>
        py.synchronized {
          // recheck so only initialise once (some other thread may have got here first)
          try py.run(eval_name)
          catch {
            case e: Throwable =>
              // variable still doesn't exist so unpack the function
              py.set(s"${eval_name}_bytes", pickleBytes)
              py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
              py.run(s"del ${eval_name}_bytes")
          }
        }
    }
    py
  }

  def invoke(args: Vector[Object]): Id[Object] = {
    val _py = initialize(py)
    _py.invoke(PyRef(eval_name), "eval_from_jvm", args)
  }
}

case class PythonFunction1[I <: AnyRef, R](pickleBytes: Array[Byte]) extends (I => Id[R]) with PythonFunction {

  override def apply(v1: I): Id[R] =
    invoke(Vector(v1)).map(v => v.asInstanceOf[R])
}

case class PythonFunction2[I <: AnyRef, J <: AnyRef, R](pickleBytes: Array[Byte])
        extends ((I, J) => Id[R])
        with PythonFunction {

  override def apply(v1: I, v2: J): Id[R] =
    invoke(Vector(v1, v2)).map(_.asInstanceOf[R])
}
