package com.raphtory.internals.management.python

import cats.Functor
import cats.syntax.all._
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.internals.management.PyRef

import scala.util.Random

class PythonGlobalStateEvaluator[IO[_]: Functor](pyObjBytes: Array[Byte], py: EmbeddedPython[IO])
        extends (GraphState => IO[Unit]) {

  private val eval_name = s"_${Random.alphanumeric.take(32).mkString}"

  py.set(s"${eval_name}_bytes", pyObjBytes)
  py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
  py.run(s"del ${eval_name}_bytes")

  override def apply(gs: GraphState): IO[Unit] =
    py.invoke(PyRef(eval_name), "eval_from_jvm", Vector(gs)).map(_ => ())

}
