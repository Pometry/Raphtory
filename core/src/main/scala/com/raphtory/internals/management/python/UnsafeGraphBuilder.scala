package com.raphtory.internals.management.python

import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.graph.GraphAlteration.{EdgeAdd, GraphUpdate, VertexAdd}
import com.raphtory.internals.management.{PyRef, PythonEncoder}

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Promise}
import scala.concurrent.duration.Duration
import scala.util.Using

class UnsafeGraphBuilder[T](ref: PyRef, py: UnsafeEmbeddedPython)(implicit PE: PythonEncoder[T])
        extends GraphBuilder[T] {

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  override def parseTuple(tuple: T): Unit = {
    py.invoke(ref, "parse_tuple", Vector(PE.encode(tuple)))
    val actions = py.eval[Vector[GraphUpdate]](s"${ref.name}.get_actions()")
    actions.collect {
      case m: VertexAdd =>
        handleVertexAdd(m)
        updateVertexAddStats()
      case m: EdgeAdd   =>
        handleEdgeAdd(m)
        updateEdgeAddStats()
    }
  }

  def parseTuple2(tuple: T): Vector[GraphUpdate] = {
    py.invoke(ref, "parse_tuple", Vector(PE.encode(tuple)))
    val actions = py.eval[Vector[GraphUpdate]](s"${ref.name}.get_actions()")
    py.invoke(ref, "reset_actions", Vector.empty)
    actions
  }
}

/**
  * spawn a thread, add a blocking queue
  * start python interpreter
  * only communicate via queue
  */
class UnsafeGraphBuilderProxy[T](queue: BlockingQueue[Msg[T]], pyThreadHandle: CompletableFuture[Void], done: AtomicBoolean)
        extends GraphBuilder[T]
        with AutoCloseable {

  def parseTuple(t: T): Unit = {
    val reply = Promise[Vector[GraphUpdate]]()
    val msg   = Msg(t, reply)
    queue.put(msg)
    Await.result(reply.future, Duration.Inf).collect {
      case m: VertexAdd =>
        handleVertexAdd(m)
        updateVertexAddStats()
      case m: EdgeAdd   =>
        handleEdgeAdd(m)
        updateEdgeAddStats()
    }

  }

  override def close(): Unit = {
    assert(done.compareAndSet(false, true))
    pyThreadHandle.get(Long.MaxValue, TimeUnit.DAYS)
    println("DONE2!")
  }
}

object UnsafeGraphBuilderProxy {

  implicit val singleThreadExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  def apply[T: PythonEncoder](): UnsafeGraphBuilderProxy[T] = {
    val done  = new AtomicBoolean(false)
    val queue = new ArrayBlockingQueue[Msg[T]](10)
    val f     = CompletableFuture.runAsync { () =>
      Using(UnsafeEmbeddedPython()) { py =>
        val builder: UnsafeGraphBuilder[T] = py.loadGraphBuilder[T]("BaseBuilder", "builder")
        while (!isDone(done)) {
          val value = queue.poll(100, TimeUnit.MILLISECONDS)
          value match {
            case null => // loopy loop
            case Msg(t: T, reply) =>
              try {
                val value1 = builder.parseTuple2(t)
                reply.success(value1)
              }
              catch {
                case t: Throwable =>
                  reply.failure(t)
              }
          }
        }
      }
      println("DONE!")
      ()
    }

    new UnsafeGraphBuilderProxy(queue, f, done)
  }

  def isDone(done: AtomicBoolean): Boolean =
    done.compareAndSet(true, true)
}

case class Msg[T](t: T, sync: Promise[Vector[GraphUpdate]])
