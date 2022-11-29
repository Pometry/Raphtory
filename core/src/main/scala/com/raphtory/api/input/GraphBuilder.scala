package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.graph.GraphBuilderF
import com.raphtory.protocol.PartitionService

trait GraphBuilder[T] extends ((Graph, T) => Unit) with Serializable {

  final def make[F[_]: Async](
      graphID: String,
      sourceID: Int,
      partitions: Map[Int, PartitionService[F]]
  ): F[GraphBuilderF[F, T]] =
    for {
      earliestSeen <- Ref.of(Long.MaxValue)
      highestSeen  <- Ref.of(Long.MinValue)
      sentUpdates  <- Ref.of(0L)
      builder      <-
        Async[F].delay(new GraphBuilderF(graphID, sourceID, this, partitions, earliestSeen, highestSeen, sentUpdates))
    } yield builder
}

private[raphtory] object GraphBuilder {

  def apply[T](parseFun: (Graph, T) => Unit): GraphBuilder[T] =
    new GraphBuilder[T] { override def apply(graph: Graph, tuple: T): Unit = parseFun(graph, tuple) }
}
