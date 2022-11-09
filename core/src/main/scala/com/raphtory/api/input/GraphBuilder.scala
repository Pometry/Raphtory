package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.graph.GraphBuilderF
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.protocol.WriterService

trait GraphBuilder[T] extends ((Graph, T) => Unit) with Serializable {

  final def make[F[_]: Async](
      graphID: String,
      sourceID: Int,
      writers: Map[Int, WriterService[F]]
  ): Resource[F, GraphBuilderF[F, T]] =
    Resource.eval(for {
      highestSeen <- Ref.of(Long.MinValue)
      sentUpdates <- Ref.of(0L)
      builder     <- Async[F].delay(new GraphBuilderF(graphID, sourceID, this, writers, highestSeen, sentUpdates))
    } yield builder)
}

private[raphtory] object GraphBuilder {

  def apply[T](parseFun: (Graph, T) => Unit): GraphBuilder[T] =
    new GraphBuilder[T] { override def apply(graph: Graph, tuple: T): Unit = parseFun(graph, tuple) }
}
