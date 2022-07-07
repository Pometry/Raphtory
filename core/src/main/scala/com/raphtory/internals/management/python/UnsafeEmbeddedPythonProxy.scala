package com.raphtory.internals.management.python

import cats.Id
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.util.Try
import scala.util.Using

class UnsafeEmbeddedPythonProxy(
    queue: ArrayBlockingQueue[Any],
    threadHandle: CompletableFuture[Void],
    done: AtomicBoolean
) extends EmbeddedPython[Id]
        with AutoCloseable { self =>

  override def invoke(ref: PyRef, methodName: String, args: Vector[Object]): Id[Unit] = {
    val reply = Promise[Any]
    queue.offer(Invoke(ref, methodName, args, reply))
    Await.result(reply.future, Duration.Inf)
  }

  override def eval[T](expr: String)(implicit PE: PythonEncoder[T]): Id[T] = {
    val reply           = Promise[T]
    val msg: PyMsg[Any] = Eval[T](expr, reply).asInstanceOf[PyMsg[Any]]
    queue.offer(msg)
    Await.result(reply.future, Duration.Inf)
  }

  override def close(): Unit = {
    assert(done.compareAndSet(false, true))
    threadHandle.get(Long.MaxValue, TimeUnit.DAYS)
  }

  override def loadGraphBuilder[T: PythonEncoder](cls: String, pkg: Option[String]): GraphBuilder[T] = {
    val reply           = Promise[GraphBuilder[T]]
    val msg: PyMsg[Any] =
      NewGraphBuilder[T](cls, pkg, reply.asInstanceOf[Promise[GraphBuilder[Any]]]).asInstanceOf[PyMsg[Any]]
    queue.offer(msg)
    Await.result(reply.future, Duration.Inf) match {
      case g: UnsafeGraphBuilder[T] =>
        new UnsafeGraphBuilder[T](g.ref, self)(PythonEncoder[T])
    }

  }

  override def run(script: String): Id[Unit] = {
    val reply = Promise[Any]
    queue.offer(Run(script, reply))
    Await.result(reply.future, Duration.Inf)
  }

  override def set(name: String, obj: Any): Id[Unit] = {
    val reply = Promise[Any]
    queue.offer(Set(name, obj, reply))
    Await.result(reply.future, Duration.Inf)
  }
}

object UnsafeEmbeddedPythonProxy {

  def apply(): UnsafeEmbeddedPythonProxy = {

    val done  = new AtomicBoolean(false)
    val queue = new ArrayBlockingQueue[Any](10)

    val f = CompletableFuture.runAsync { () =>
      Using(UnsafeEmbeddedPython()) { py =>
        while (!isDone(done)) {
          val value = queue.poll(100, TimeUnit.MILLISECONDS)
          value match {
            case null                                        => // loopy loop
            case Set(name, obj, reply) =>
              reply.complete(Try(py.set(name, obj)))
            case Run(script, reply)                          =>
              reply.complete(Try(py.run(script)))
            case ngb: NewGraphBuilder[Any]                   =>
              ngb.reply.complete(Try(py.loadGraphBuilder[Any](ngb.cls, ngb.pkg)(ngb.PE)))
            case e: Eval[Any] @unchecked                     =>
              e.reply.complete(Try(py.eval(e.expr)(e.PE)))
            case Invoke(ref: PyRef, methodName, args, reply) =>
              reply.complete(Try(py.invoke(ref, methodName, args)))
          }
        }
      }
      ()
    }

    new UnsafeEmbeddedPythonProxy(queue, f, done)
  }

  def isDone(done: AtomicBoolean): Boolean =
    done.compareAndSet(true, true)
}

sealed trait PyMsg[T] {
  def reply: Promise[T]
}

case class Invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty, reply: Promise[Any])
        extends PyMsg[Any]

case class Eval[T](expr: String, reply: Promise[T])(implicit val PE: PythonEncoder[T]) extends PyMsg[T]

case class NewGraphBuilder[T](cls: String, pkg: Option[String], reply: Promise[GraphBuilder[Any]])(implicit
    val PE: PythonEncoder[T]
) extends PyMsg[GraphBuilder[Any]]

case class Run(script: String, reply: Promise[Any]) extends PyMsg[Any]
case class Set(script: String, obj:Any, reply: Promise[Any]) extends PyMsg[Any]
