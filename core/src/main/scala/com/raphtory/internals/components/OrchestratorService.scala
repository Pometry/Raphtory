package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Concurrent
import cats.effect.Ref
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.protocol.GraphInfo
import com.raphtory.protocol.Status
import com.raphtory.protocol.success

abstract class OrchestratorService[F[_]: Concurrent, T](graphs: GraphList[F, T]) {

  // Methods to override by instances of this class
  protected def makeGraphData(graphId: String): F[T]

  protected def graphExecution(graph: Graph[F, T]): F[Unit] = Concurrent[F].unit

  final def establishGraph(req: GraphInfo): F[Status] =
    for {
      data                 <- makeGraphData(req.graphId)
      supervisorAllocated  <- Supervisor[F].allocated
      (supervisor, release) = supervisorAllocated
      graph                 = Graph(req.graphId, supervisor, release, data)
      _                    <- supervisor.supervise(graphExecution(graph))
      _                    <- graphs.update(graphs => graphs + (req.graphId -> graph))
    } yield success

  final def destroyGraph(req: GraphInfo): F[Status] = destroyGraph(req.graphId).as(success)

  final protected def attachExecutionToGraph(graphId: String, execution: Graph[F, T] => F[Unit]): F[Unit] =
    for {
      graph <- graphs.get.map(graphs => graphs(graphId))
      _     <- graph.supervisor.supervise(execution(graph))
    } yield ()

  final protected def destroyGraph(graphId: String): F[Unit] =
    for {
      graph <- graphs.modify(graphs => (graphs - graphId, graphs(graphId)))
      _     <- graph.release
    } yield ()
}

object OrchestratorService {
  case class Graph[F[_], T](id: String, supervisor: Supervisor[F], release: F[Unit], data: T)
  type GraphList[F[_], T] = Ref[F, Map[String, Graph[F, T]]]
}
