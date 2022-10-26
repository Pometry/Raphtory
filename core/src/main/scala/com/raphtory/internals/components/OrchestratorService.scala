package com.raphtory.internals.components

import cats.effect.Concurrent
import cats.effect.Ref
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.protocol.GraphInfo
import com.raphtory.protocol.Status
import com.raphtory.protocol.success

abstract class OrchestratorService[F[_]: Concurrent](graphs: GraphList[F]) {

  def establishGraph(req: GraphInfo): F[Status] =
    for {
      supervisorAllocated  <- Supervisor[F].allocated
      (supervisor, release) = supervisorAllocated
      graph                 = Graph[F](req.graphId, supervisor, release)
      _                    <- graphs.update(graphs => graphs + (req.graphId -> graph))
    } yield success

  def destroyGraph(req: GraphInfo): F[Status] = destroyGraph(req.graphId).as(success)

  protected def attachExecutionToGraph(graphId: String, execution: F[Unit]): F[Unit] =
    for {
      graph <- graphs.get.map(graphs => graphs(graphId))
      _     <- graph.supervisor.supervise(execution)
    } yield ()

  protected def destroyGraph(graphId: String): F[Unit] =
    for {
      graph <- graphs.modify(graphs => (graphs - graphId, graphs(graphId)))
      _     <- graph.release
    } yield ()
}

object OrchestratorService {
  case class Graph[F[_]](id: String, supervisor: Supervisor[F], release: F[Unit])
  type GraphList[F[_]] = Ref[F, Map[String, Graph[F]]]
}
