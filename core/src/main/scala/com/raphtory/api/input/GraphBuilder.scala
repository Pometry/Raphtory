package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Dispatcher
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.protocol.WriterService

trait GraphBuilder[T] extends ((Graph, T) => Unit) with Serializable {

  final def make[F[_]: Async](
      graphID: String,
      sourceID: Int,
      writers: Map[Int, WriterService[F]]
  ): Resource[F, GraphBuilderInstance[F, T]] =
    Dispatcher[F].map(dispatcher => new GraphBuilderInstance[F, T](graphID, sourceID, this, dispatcher, writers))
}

private[raphtory] object GraphBuilder {

  def apply[T](parseFun: (Graph, T) => Unit): GraphBuilder[T] =
    new GraphBuilder[T] { override def apply(graph: Graph, tuple: T): Unit = parseFun(graph, tuple) }
}
