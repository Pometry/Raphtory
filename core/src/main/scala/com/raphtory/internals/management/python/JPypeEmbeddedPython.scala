package com.raphtory.internals.management.python

import cats.Id
import com.raphtory.internals.management.{PyRef, PythonEncoder}
import pemja.core.Interpreter

case class JPypeEmbeddedPython(interpreter: Interpreter) extends EmbeddedPython[Id] {
  override def invoke(ref: PyRef, methodName: String, args: Vector[Object]): Id[Object] =
    interpreter.invokeMethod(ref.name, methodName, args: _*)

  override def eval[T](expr: String)(implicit PE: PythonEncoder[T]): Id[T] = ???

  override def run(script: String): Id[Unit] = interpreter.exec(script)

  override def set(name: String, obj: Any): Id[Unit] = interpreter.set(name, obj)
}
