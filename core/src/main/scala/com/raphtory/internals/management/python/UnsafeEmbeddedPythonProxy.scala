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

  override def loadGraphBuilder[T: PythonEncoder](cls: String, pkg: String): GraphBuilder[T] = {
    val reply           = Promise[GraphBuilder[T]]
    val msg: PyMsg[Any] =
      NewGraphBuilder[T](cls, pkg, reply.asInstanceOf[Promise[GraphBuilder[Any]]]).asInstanceOf[PyMsg[Any]]
    queue.offer(msg)
    Await.result(reply.future, Duration.Inf) match {
      case g: UnsafeGraphBuilder[T] =>
        new UnsafeGraphBuilder[T](g.ref, self)(PythonEncoder[T])
    }

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
            case ngb: NewGraphBuilder[Any]                   =>
              try {
                val gb = py.loadGraphBuilder[Any](ngb.cls, ngb.pkg)(ngb.PE)
                ngb.reply.success(gb)
              }
              catch {
                case t: Throwable =>
                  ngb.reply.failure(t)
              }

            case e: Eval[Any] @unchecked                     =>
              try {
                val res = py.eval(e.expr)(e.PE)
                e.reply.success(res)
              }
              catch {
                case t: Throwable =>
                  e.reply.failure(t)

              }
            case Invoke(ref: PyRef, methodName, args, reply) =>
              try {
                py.invoke(ref, methodName, args)
                reply.success(())
              }
              catch {
                case t: Throwable =>
                  reply.failure(t)
              }
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

case class NewGraphBuilder[T](cls: String, pkg: String, reply: Promise[GraphBuilder[Any]])(implicit
    val PE: PythonEncoder[T]
) extends PyMsg[GraphBuilder[Any]]
