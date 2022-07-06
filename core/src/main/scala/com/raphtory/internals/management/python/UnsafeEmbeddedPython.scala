package com.raphtory.internals.management.python

import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder
import pemja.core.PythonInterpreter
import pemja.core.PythonInterpreterConfig

import java.nio.file.Path
import java.nio.file.Paths

class UnsafeEmbeddedPython(py: PythonInterpreter, private var i: Int = 0) extends AutoCloseable { self =>

  def loadGraphBuilder[T: PythonEncoder](cls: String, pkg: String): UnsafeGraphBuilder[T] = {
    py.exec(s"import $pkg")
    val name = s"pyref_$i"
    i += 1
    py.exec(s"$name = $pkg.$cls()")
    new UnsafeGraphBuilder[T](PyRef(name), self)
  }

  private[management] def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty) =
    py.invokeMethod(ref.name, methodName, args: _*)

  private[management] def eval[T](expr: String)(implicit PE: PythonEncoder[T]): T = {
    val name  = s"tmp_$i"
    i += 1
    py.exec(s"$name = $expr")
    val pyObj = py.get(name, PE.clz.asSubclass(classOf[Object]))
    py.exec(s"del $name")
    PE.decode(pyObj)
  }

  override def close(): Unit = py.close()
}

object UnsafeEmbeddedPython {

  def defaultPaths: Seq[Path] =
    Vector(
            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory"),
            Paths.get("/home/murariuf/.virtualenvs/raphtory/lib/python3.8/site-packages")
    )

  def apply(pythonPaths: Path*): UnsafeEmbeddedPython = {
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
