package com.raphtory.internals.management.python

import cats.Id
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder
import pemja.core.PythonInterpreter
import pemja.core.PythonInterpreterConfig
import scala.language.postfixOps
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.Try
import scala.util.Using
import scala.util.control.NonFatal

class UnsafeEmbeddedPython(py: PythonInterpreter, private var i: Int = 0)
        extends EmbeddedPython[Id]
        with AutoCloseable { self =>

  def loadGraphBuilder[T: PythonEncoder](cls: String, pkg: Option[String]): Id[GraphBuilder[T]] = {
    pkg.foreach(pkg => py.exec(s"from $pkg import $cls")) // import the class if it's in a package
    val name: String = newVar
    py.exec(s"$name = $cls()")
    new UnsafeGraphBuilder[T](PyRef(name), self)
  }

  def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty): Id[Object] =
    py.invokeMethod(ref.name, methodName, args: _*)

  def eval[T](expr: String)(implicit PE: PythonEncoder[T]): Id[T] =
    Using(tmpVar) {
      case TmpVar(name) =>
        py.exec(s"$name = $expr")
        val pyObj = py.get(name, PE.clz.asSubclass(classOf[Object]))
        PE.decode(pyObj)
    }.get

  def run(script: String): Id[Unit] =
    py.exec(script)

  override def close(): Unit = py.close()

  private def tmpVar = {
    val name = s"tmp_$i"
    i += 1
    TmpVar(name)
  }

  private def newVar = {
    val name = s"pyref_$i"
    i += 1
    name
  }

  case class TmpVar(name: String) extends AutoCloseable {

    override def close(): Unit =
      py.exec(s"del $name")
  }

  override def set(name: String, obj: Any): Id[Unit] =
    py.set(name, obj)
}

object UnsafeEmbeddedPython {

  def apply(pythonPaths: Path*): UnsafeEmbeddedPython = {
    import sys.process._

    try {
      val pythonExec: String = Try(sys.env("PYEXEC"))
        .orElse(Try("which python3" !!))
        .getOrElse(
                throw new IllegalArgumentException("Unable to find python3 on path or via environment variable PYEXEC")
        )
        .trim

      val sitePackages = Try(s"""$pythonExec -c 'import site; print(" ".join(site.getsitepackages()))'""" !!)
        .map(res => res.trim.split(" ").map(Paths.get(_)))
        .get

      val builder =
        PythonInterpreterConfig
          .newBuilder()
          .setPythonExec(pythonExec)
          .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)

      val config      = (pythonPaths ++ sitePackages)
        .foldLeft(builder) { (b, path) =>
          b.addPythonPaths(path.toAbsolutePath.toString)
        }
        .build()
      val interpreter = new PythonInterpreter(config)
      new UnsafeEmbeddedPython(interpreter)
    }
    catch {
      case NonFatal(t) =>
        t.printStackTrace()
        throw t
    }
  }
}
