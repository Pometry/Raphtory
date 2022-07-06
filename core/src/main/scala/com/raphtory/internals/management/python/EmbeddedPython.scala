package com.raphtory.internals.management.python

import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder

trait EmbeddedPython[IO[_]] {

  def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty): IO[Unit]

  def eval[T](expr: String)(implicit PE: PythonEncoder[T]): IO[T]

  def loadGraphBuilder[T: PythonEncoder](cls: String, pkg: String): GraphBuilder[T]
}
