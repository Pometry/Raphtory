package com.raphtory.internals.management.python

import cats.Functor
import cats.syntax.all._
import com.raphtory.api.analysis.table.Row
import com.raphtory.internals.management.PyRef

import scala.util.Random

class PythonExplodeEvaluator[IO[_]: Functor](pyObjBytes: Array[Byte], py: EmbeddedPython[IO])
        extends (Row => IO[IterableOnce[Row]]) {

  private val eval_name = s"_${Random.alphanumeric.take(32).mkString}"
  py.set(s"${eval_name}_bytes", pyObjBytes)
  py.run(s"import cloudpickle as pickle; $eval_name = pickle.loads(${eval_name}_bytes)")
  py.run(s"del ${eval_name}_bytes")

  override def apply(r: Row): IO[IterableOnce[Row]]  =
    py.invoke(PyRef(eval_name), "eval_from_jvm", Vector(r)).map {
      case arr: Array[Any] @unchecked => arr.map(x => Row(x.asInstanceOf[Array[Any]]: _*))
    }
}
