package com.raphtory.internals.management.python

import cats.Id
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.management.{PyRef, PythonEncoder, PythonInterop}
import pemja.core.{PythonInterpreter, PythonInterpreterConfig}

import java.nio.file.{Path, Paths}
import scala.util.Using

class UnsafeEmbeddedPython(py: PythonInterpreter, private var i: Int = 0)
        extends EmbeddedPython[Id]
        with AutoCloseable { self =>

  def loadGraphBuilder[T:PythonEncoder](cls: String, pkg: String): Id[GraphBuilder[T]] = {
    py.exec(s"import $pkg")
    val name: String = newVar
    py.exec(s"$name = $pkg.$cls()")
    new UnsafeGraphBuilder[T](PyRef(name), self)
  }

  def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty): Id[Unit] =
    py.invokeMethod(ref.name, methodName, args: _*)

  def eval[T](expr: String)(implicit PE: PythonEncoder[T]): Id[T] =
    Using(tmpVar) {
      case TmpVar(name) =>
        py.exec(s"$name = $expr")
        val pyObj = py.get(name, PE.clz.asSubclass(classOf[Object]))
        PE.decode(pyObj)
    }.get

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
}

object UnsafeEmbeddedPython {

  def defaultPaths: Seq[Path] =
    Vector(
            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory"),
            Paths.get("/home/murariuf/.virtualenvs/raphtory/lib/python3.8/site-packages")
    )

  def apply(pythonPaths: Path*): UnsafeEmbeddedPython = {
    //
    val x = PythonInterop.assignId("zx")
    val builder =
      PythonInterpreterConfig
        .newBuilder()
        .setPythonExec("/home/murariuf/.virtualenvs/raphtory/bin/python3")
        .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)

    val config = (pythonPaths ++ defaultPaths)
      .foldLeft(builder) { (b, path) =>
        b.addPythonPaths(path.toAbsolutePath.toString)
      }
      .build()
    new UnsafeEmbeddedPython(new PythonInterpreter(config))
  }
}
