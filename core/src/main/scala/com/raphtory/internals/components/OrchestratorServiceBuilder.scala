package com.raphtory.internals.components

import cats.Monad
import cats.effect.Concurrent
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait OrchestratorServiceBuilder {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def makeGraphList[F[_]: Concurrent, T]: Resource[F, GraphList[F, T]] =
    Resource.make(Ref.of(Map[String, Graph[F, T]]()))(graphs => releaseAllGraphs(graphs))

  private def releaseAllGraphs[F[_]: Monad, T](graphs: GraphList[F, T]): F[Unit] =
    for {
      graphs <- graphs.modify(graphs => (Map(), graphs)) // Empty the list and retrieve the graphs on it
      _      <- graphs.values.map(graph => graph.release).toList.sequence // Sequence the releasing
    } yield ()
}
