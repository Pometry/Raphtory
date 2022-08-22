package com.raphtory.internals.management

import cats.Id
import cats.syntax.all._
import java.lang.reflect.{Array => JArray}
import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.management.python.EmbeddedPython
import com.raphtory.internals.management.python.UnsafeEmbeddedPythonProxy
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.Random

/** Scala-side methods for interfacing with Python */
object PythonInterop {
  val logger: WrappedLogger = new WrappedLogger(Logger(LoggerFactory.getLogger(this.getClass)))

  def print_array(array: Array[_]): String =
    array.mkString(", ")

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

  /** Take an iterable of arguments and turn it into a java varargs friendly array */
  def make_varargs[T](obj: Iterable[T], clazz: Class[Array[T]]): Array[T] = {
    val comp = clazz.getComponentType
    val arr  = JArray.newInstance(comp, obj.size).asInstanceOf[Array[T]]
    obj.zipWithIndex.foreach {
      case (v, i) =>
        arr(i) = v
    }
    arr
  }

  /** Find methods and default values for an object and return in friendly format */
  def methods(obj: Any): util.Map[String, Array[Method]] = {
    logger.trace(s"Scala 'methods' called with $obj")
    val prefixedMethodDict         = mutable.Map.empty[String, mutable.ArrayBuffer[java.lang.reflect.Method]]
    val prefixedMethodDefaultsDict = mutable.Map.empty[String, mutable.Map[Int, java.lang.reflect.Method]]
    obj.getClass.getMethods.foreach { m =>
      val parts: Array[String] = """\$default\$""".r.split(m.getName)
      val prefix               = camel_to_snake(parts(0))

      if (parts.length == 1)
        // ordinary method
        prefixedMethodDict.getOrElseUpdate(prefix, mutable.ArrayBuffer.empty[java.lang.reflect.Method]).append(m)
      else {
        // method encapsulating default argument
        val defaultIndex = parts(1).toInt - 1
        prefixedMethodDefaultsDict
          .getOrElseUpdate(prefix, mutable.Map.empty[Int, java.lang.reflect.Method])
          .addOne(defaultIndex, m)
      }
    }
    val res                        = prefixedMethodDict
      .map {
        case (name, methods) =>
          val defaults = prefixedMethodDefaultsDict.get(name) match {
            case Some(v) => v.toMap
            case None    => Map.empty[Int, java.lang.reflect.Method]
          }

          name -> methods.map { m =>
            val hasVarArgs  = m.isVarArgs
            val paramsNames = m.getParameters.map(p => camel_to_snake(p.getName))
            val paramTypes  = m.getParameterTypes

            val n = m.getParameterCount
            // Only one overloaded implementation can have default arguments, this checks if defaults should apply
            if (
                    defaults.forall {
                      case (i, d) => i < m.getParameterCount && m.getParameterTypes()(i) == d.getReturnType
                    }
            )
              // All default arguments match parameter types of this method signature
              Method(m.getName, n, paramsNames, paramTypes, defaults.view.mapValues(_.getName).toMap, hasVarArgs)
            else
              // Defaults do not match or method has no default arguments
              Method(m.getName, n, paramsNames, paramTypes, Map.empty[Int, String], hasVarArgs)
          }.toArray
      }
      .toMap
      .asJava
    logger.trace(s"Returning found methods for $obj")
    res
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
    types: Array[Class[_]],
    defaults: Map[Int, String],
    varargs: Boolean
) {
  def has_defaults: Boolean = defaults.nonEmpty
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

  private def py: EmbeddedPython[Id] = UnsafeEmbeddedPythonProxy.global

  private def initialize(py: EmbeddedPython[Id]) =
    synchronized {
      try py.run(eval_name)
      catch {
        case e: Throwable =>
          py.set(s"${eval_name}_bytes", pickleBytes)
          py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
          py.run(s"del ${eval_name}_bytes")
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
