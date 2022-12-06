package com.raphtory.internals.management

import cats.Id
import cats.syntax.all._

import java.lang.reflect.{Array => JArray}
import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Property
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.internals.management.python.EmbeddedPython
import com.raphtory.internals.management.python.JPypeEmbeddedPython
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe
import scala.reflect.api.Types
import universe._
import scala.util.Random
import com.github.takezoe.scaladoc.Scaladoc
import com.raphtory.api.analysis.algorithm.BaseAlgorithm
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker.ProgressTracker
import com.raphtory.internals.context.RaphtoryContext
import pemja.core.Interpreter
import sun.misc.Unsafe

import scala.annotation.tailrec
import scala.language.existentials

/** Scala-side methods for interfacing with Python */
object PythonInterop {
  val logger: WrappedLogger = new WrappedLogger(Logger(LoggerFactory.getLogger(this.getClass)))

  def disableReflectWarning(): Unit =
    try {
      val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
      theUnsafe.setAccessible(true)
      val u         = theUnsafe.get(null).asInstanceOf[Unsafe]
      val cls       = Class.forName("jdk.internal.module.IllegalAccessLogger")
      val logger    = cls.getDeclaredField("logger")
      u.putObjectVolatile(cls, u.staticFieldOffset(logger), null)
    }
    catch {
      case e: Exception => println(e.getStackTrace.mkString("Array(", ", ", ")"))
    }

  disableReflectWarning()

  def repr(obj: Any): String =
    obj match {
      case v: Array[_]                                       =>
        "[" + v.map(repr).mkString(", ") + "]"
      case v: Map[_, _]                                      =>
        "{" + v.map(t => repr(t._1) + ": " + repr(t._2)).mkString(", ") + "}"
      case v: Iterable[_]                                    =>
        "[" + v.map(repr).mkString(", ") + "]"
      case v: Product if v.productPrefix.startsWith("Tuple") =>
        "(" + v.productIterator.map(repr).mkString(", ") + ")"

      case v: Product                                        =>
        v.productPrefix + "(" + v.productElementNames
          .zip(v.productIterator)
          .map {
            case (name, value) => s"$name=${repr(value)}"
          }
          .mkString(", ") + ")"
      case v                                                 => v.toString
    }

  /** Get scaladoc string from annotation for class
    */
  def docstring_for_class(obj: Any): String =
    Option(obj.getClass.getAnnotation(classOf[Scaladoc]).value()).getOrElse("")

  /** make assign_id accessible from python */
  def assign_id(s: String): Long =
    Graph.assignID(s)

  def set_interpreter(interpreter: Interpreter): Unit =
    EmbeddedPython.injectInterpreter(JPypeEmbeddedPython(interpreter))

  /** convert names from camel to snake case */
  def camel_to_snake(s: String): String =
    PythonEncoder.camelToSnakeCase(s)

  /** used to convert java objects to scala objects when passing through python collections
    * (define more converters as needed)
    */
  def decode[T](obj: java.util.Collection[_]): T =
    obj.asScala.asInstanceOf[T]

  /** create a Scala tuple from python (nasty hack necessary due to Scala type safety and non-iterable tuples) */
  def decode_tuple[T](obj: java.util.Collection[_]): T = {
    val objScala = obj.asScala.toSeq
    val class_   = Class.forName("scala.Tuple" + objScala.size)
    class_.getConstructors.apply(0).newInstance(objScala: _*).asInstanceOf[T]
  }

  def decode[T](obj: java.util.Map[_, _]): T =
    obj.asScala.asInstanceOf[T]

  /** Look up name of python wrapper based on input type
    * (used to provide specialised wrappers for categories of types rather than specific classes)
    */
  def get_wrapper_str(obj: Any): String =
    obj match {
      case _: Array[_]             => "Array"
      case _: collection.Map[_, _] => "Mapping"
      case _: collection.Seq[_]    => "Sequence"
      case _: Iterable[_]          => "Iterable"
      case _: Iterator[_]          => "Iterator"
      case _: GraphPerspective     => "TemporalGraph"
      case _: Graph                => "Graph"
      case _: Table                => "Table"
      case _: Accumulator[_, _]    => "Accumulator"
      case _: Vertex               => "Vertex"
      case _: GraphState           => "GraphState"
      case _                       => "None"
    }

  private def funTypeRepr(v: universe.Type, tpe: universe.Type): String = {
    val f         = tpe.typeSymbol.asClass
    val n         = f.typeParams.size - 1
    val argsTypes = f.typeParams.map(t => t.asType.toType.asSeenFrom(v, f))
    val argIter   = argsTypes.grouped(n)
    val args      = argIter.next()
    val ret       = argIter.next().head
    "Callable[[" + args.map(get_type_repr).mkString(", ") + "], " + get_type_repr(ret) + "]"
  }

  def integralType(tpe: universe.Type): Boolean =
    (tpe weak_<:< typeOf[Long]) ||
      tpe =:= typeOf[java.lang.Byte] ||
      tpe =:= typeOf[java.lang.Short] ||
      tpe =:= typeOf[java.lang.Integer] ||
      tpe =:= typeOf[java.lang.Long]

  def floatType(tpe: universe.Type): Boolean =
    tpe =:= typeOf[Float] || tpe =:= typeOf[Double] ||
      tpe =:= typeOf[java.lang.Float] || tpe =:= typeOf[java.lang.Double]

  def stringType(tpe: universe.Type): Boolean =
    tpe =:= typeOf[String]

  def booleanType(tpe: universe.Type): Boolean =
    tpe =:= typeOf[Boolean] || tpe =:= typeOf[java.lang.Boolean]

  def bytesType(tpe: universe.Type): Boolean =
    tpe =:= typeOf[Array[Byte]]

  def arrayType(tpe: universe.Type): Boolean =
    !bytesType(tpe) && tpe <:< typeOf[Array[_]]

  def pyListType(tpe: universe.Type): Boolean =
    arrayType(tpe) || (typeOf[mutable.Buffer[_]].erasure <:< tpe.erasure && tpe <:< typeOf[IterableOnce[_]])

  def iteratorType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[IterableOnce[_]] && typeOf[Iterator[_]].erasure <:< tpe.erasure

  def iterableType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[IterableOnce[_]] && typeOf[Iterable[_]].erasure <:< tpe.erasure

  def sequenceType(tpe: universe.Type): Boolean =
    !iterableType(tpe) && (tpe <:< typeOf[Iterable[_]] && typeOf[collection.Seq[_]].erasure <:< tpe.erasure)

  def varargsType(tpe: universe.Type): Boolean =
    tpe.toString.endsWith("*")

  def function1Type(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Function1[_, _]] && !(tpe <:< typeOf[Iterable[_]]) &&
      typeOf[PythonFunction1[_, _]].erasure <:< tpe.erasure

  def function2Type(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Function2[_, _, _]] && typeOf[PythonFunction2[_, _, _]].erasure <:< tpe.erasure

  def scalaListType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Seq[_]] && typeOf[List[_]].erasure <:< tpe.erasure && !varargsType(tpe)

  def scalaMappingType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[collection.Map[_, _]] && typeOf[immutable.Map[_, _]].erasure <:< tpe.erasure

  def genericType(tpe: universe.Type): Boolean =
//    tpe.typeSymbol.asType.isExistential
    false // need to figure out how to detect these if we want them

  def graphPerspectiveType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[GraphPerspective]

  def ingestionGraphType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Graph] && !graphPerspectiveType(tpe)

  def vertexType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Vertex]

  def graphStateType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[GraphState]

  def accumulatorType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Accumulator[_, _]]

  def spoutType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Spout[_]]

  def sourceType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Source]

  def sinkType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Sink]

  def propertyType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Property]

  def rowType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Row]

  def tableType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Table]

  def progressTrackerType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[ProgressTracker]

  def formatType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[Format]

  def contextType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[RaphtoryContext]

  def algorithmType(tpe: universe.Type): Boolean =
    tpe <:< typeOf[BaseAlgorithm]

  def extractTypeArgs(tpe: universe.Type, parent: universe.Type): Iterable[String] = {
    val clazz = parent.typeSymbol.asClass
    clazz.typeParams
      .map { t =>
        val T = t.asType.toType
        get_type_repr(T.asSeenFrom(tpe, clazz))
      }
  }

  def typeArgRepr(tpe: universe.Type, parent: universe.Type): String =
    extractTypeArgs(tpe, parent).mkString("[", ", ", "]")

  // try to remove all aliases and singleton types
  @tailrec
  def expandType(tpe: universe.Type): universe.Type = {
    val widened = tpe.widen.dealias
    if (widened != tpe)
      expandType(widened)
    else
      tpe
  }

  def get_type_repr(tpe: universe.Type): String = {
    val collectionStr: String                                           = "scala.collection."
    val reprMap: Map[universe.Type => Boolean, universe.Type => String] = Map(
            (bytesType, _ => "bytes"),
            (arrayType, tpe => collectionStr + "Array" + typeArgRepr(tpe, typeOf[Array[_]])),
            (integralType, _ => "int"),
            (floatType, _ => "float"),
            (stringType, _ => "str"),
            (booleanType, _ => "bool"),
            (varargsType, tpe => extractTypeArgs(tpe, typeOf[Seq[_]]).head),
            (pyListType, tpe => "list" + typeArgRepr(tpe, typeOf[IterableOnce[_]])),
            (iteratorType, tpe => collectionStr + "Iterator" + typeArgRepr(tpe, typeOf[IterableOnce[_]])),
            (iterableType, tpe => collectionStr + "Iterable" + typeArgRepr(tpe, typeOf[IterableOnce[_]])),
            (sequenceType, tpe => collectionStr + "Sequence" + typeArgRepr(tpe, typeOf[Iterable[_]])),
            (scalaListType, tpe => collectionStr + "List" + typeArgRepr(tpe, typeOf[List[_]])),
            (scalaMappingType, tpe => collectionStr + "Mapping" + typeArgRepr(tpe, typeOf[collection.Map[_, _]])),
            (function1Type, tpe => funTypeRepr(tpe, typeOf[Function1[_, _]])),
            (function2Type, tpe => funTypeRepr(tpe, typeOf[Function2[_, _, _]])),
            (ingestionGraphType, tpe => "graph.Graph"),
            (graphPerspectiveType, tpe => "graph.TemporalGraph"),
            (vertexType, tpe => "vertex.Vertex"),
            (graphStateType, tpe => "vertex.GraphState"),
            (accumulatorType, tpe => "graph.Accumulator"),
            (spoutType, tpe => "spouts.Spout"),
            (sourceType, tpe => "sources.Source"),
            (sinkType, tpe => "sinks.Sink"),
            (propertyType, tpe => "input.Property"),
            (rowType, tpe => "graph.Row"),
            (tableType, tpe => "graph.Table"),
            (progressTrackerType, tpe => "graph.ProgressTracker"),
            (formatType, tpe => "formats.Format"),
            (contextType, tpe => "context.PyRaphtory"),
            (algorithmType, tpe => "algorithm.PyAlgorithm"),
            (algorithmType, tpe => "algorithm.ScalaAlgorithm"),
            (genericType, tpe => tpe.toString)
    )
    val options: immutable.Iterable[String]                             = reprMap.collect {
      case (match_fun, repr_fun) if match_fun(expandType(tpe)) => repr_fun(tpe)
    }
    val r                                                               =
      if (options.isEmpty)
        "Any"
      else if (options.size == 1)
        options.head
      else
        options.mkString("Union[", ", ", "]")

    r
  }

  /** Find the singleton instance of a companion object for a class name
    * (used for constructing objects from python)
    */
  def find_class(name: String): Any = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    try {
      val module = runtimeMirror.staticModule(name)
      val obj    = runtimeMirror.reflectModule(module)
      obj.instance
    }
    catch {
      case v: ClassNotFoundException => ()
    }
  }

  /** Take an iterable of arguments and turn it into a varargs friendly list */
  def make_varargs[T](obj: Iterable[T]): List[T] =
    obj.toList

  private def getMethodDocString(method: MethodSymbol): String =
    getDocStringFromAnnotations(method.annotations).getOrElse {
      method.overrides.iterator
        .map(m => getDocStringFromAnnotations(m.annotations))
        .collectFirst {
          case Some(docs) => docs
        }
        .getOrElse("")
    }

  private def getDocStringFromAnnotations(annotations: Seq[universe.Annotation]): Option[String] =
    annotations.flatMap { annotation =>
      if (annotation.tree.tpe =:= typeOf[Scaladoc]) {
        val doc = annotation.tree.children(1).children(1) match {
          case Literal(Constant(doc: String)) => doc
        }
        Some(doc)
      }
      else
        None
    }.headOption

  private def public_methods(clazz: Class[_]): Map[String, ArrayBuffer[Method]] = {
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
      m.overrides
      val name       = m.name.toString
      val params     = m.paramLists.flatten
      val types      = params.map(p => p.infoIn(objType)).toArray
      val implicits  = params.collect { case p if p.isImplicit => camel_to_snake(p.name.toString) }.toArray
      val paramNames = params.filterNot(_.isImplicit).map(p => camel_to_snake(p.name.toString)).toArray
      val docs       = getMethodDocString(m)
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
                        implicits,
                        docs
                )
        )
    }
    methodMap.toMap
  }

  private def methodsForClass(clazz: Class[_]): util.Map[String, Array[Method]] = {
    val publicMethods = public_methods(clazz)
    val javaMethods   = clazz.getMethods.map(m => m.getName).toSet
    val actual        = publicMethods.view
      .filterKeys(m => javaMethods contains m)
      .map { case (key, value) => camel_to_snake(key) -> value.toArray }
      .toMap
      .asJava
    actual
  }

  def methods_from_name(name: String): util.Map[String, Array[Method]] =
    try {
      val clazz = Class.forName(name)
      methodsForClass(clazz)
    }
    catch {
      case _: ClassNotFoundException => new util.HashMap[String, Array[Method]]()
      case e                         => throw e
    }

  /** Find methods and default values for an object and return in friendly format */
  def methods(obj: Any): util.Map[String, Array[Method]] = {
    val clazz = obj.getClass
    logger.trace(s"Scala 'methods' called with $obj")
    methodsForClass(clazz)
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
    types: Array[Type],
    defaults: java.util.Map[Int, String],
    varargs: Boolean,
    implicits: Array[String],
    docs: String = ""
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
              println(s"unpacking $eval_name")
              py.set(s"${eval_name}_bytes", pickleBytes)
              py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
              println(s"deleting packed bytes for $eval_name")
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

case class PythonFunction1[I <: AnyRef, R](pickleBytes: Array[Byte], override protected val eval_name: String)
        extends (I => Id[R])
        with PythonFunction {

  override def apply(v1: I): Id[R] =
    invoke(Vector(v1)).map(v => v.asInstanceOf[R])
}

case class PythonFunction2[I <: AnyRef, J <: AnyRef, R](
    pickleBytes: Array[Byte],
    override protected val eval_name: String
) extends ((I, J) => Id[R])
        with PythonFunction {

  override def apply(v1: I, v2: J): Id[R] =
    invoke(Vector(v1, v2)).map(_.asInstanceOf[R])
}
