package com.raphtory.internals.management.python

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.management.PyRef

import scala.util.Random

class PythonIterateEvaluator[IO[_]](pyObjBytes: Array[Byte], py: EmbeddedPython[IO])
        extends (Vertex => IO[Unit])
        with AutoCloseable {

  private val eval_name = s"_${Random.alphanumeric.take(32).mkString}"
  py.set(s"${eval_name}_bytes", pyObjBytes)
  py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
  py.run(s"del ${eval_name}_bytes")

  override def apply(v: Vertex): IO[Unit] =
    py.invoke(PyRef(eval_name), "eval_from_jvm", Vector(new PythonVertex(v)))

  override def close(): Unit = {}
}
