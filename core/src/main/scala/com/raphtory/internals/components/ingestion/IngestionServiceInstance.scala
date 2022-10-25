package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRepository
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.TryIngestData
import com.raphtory.protocol
import com.raphtory.protocol.ClientGraphId
import com.raphtory.protocol.DestroyGraph
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.Status
import com.raphtory.protocol.failure
import com.raphtory.protocol.success
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object IngestionServiceInstance {
  case class Graph[F[_]](supervisor: Supervisor[F], release: F[Unit], clients: Set[String])

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](repo: ServiceRepository[F], config: Config): Resource[F, Unit] =
    for {
      _       <- Resource.eval(Async[F].delay(logger.info(s"Starting Ingestion Service")))
      graphs  <- Resource.make(Ref.of(Map[String, Graph[F]]()))(graphs => releaseAllGraphs(graphs))
      service <- Resource.eval(Async[F].delay(new IngestionServiceInstance[F](graphs, repo.topics, config)))
      _       <- repo.registered(service, IngestionServiceInstance.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, IngestionService[F]] =
    GrpcServiceDescriptor[F, IngestionService[F]](
            "ingestion",
            IngestionService.client(_),
            IngestionService.bindService(Async[F], _)
    )

  private def releaseAllGraphs[F[_]: Async](graphs: Ref[F, Map[String, Graph[F]]]): F[Unit] =
    for {
      graphs <- graphs.modify(graphs => (Map(), graphs)) // Empty the list and retrieve the graphs on it
      _      <- graphs.values.foldLeft(Async[F].unit)((io, graph) => io *> graph.release) // Sequence the releasing
    } yield ()
}

class IngestionServiceInstance[F[_]: Async](
    graphs: Ref[F, Map[String, IngestionServiceInstance.Graph[F]]],
    repo: TopicRepository,
    config: Config
) extends IngestionService[F] {
  import IngestionServiceInstance.Graph

  override def establishGraph(req: ClientGraphId): F[Status] =
    for {
      supervisorAllocated  <- Supervisor[F].allocated
      (supervisor, release) = supervisorAllocated
      graph                 = Graph[F](supervisor, releaseGraph(req.graphId, release), Set(req.clientId))
      alreadyCreated       <- graphs.modify { graphs =>
                                if (graphs.isDefinedAt(req.graphId)) (graphs, true)
                                else (graphs + (req.graphId -> graph), false)
                              }
      // Release the supervisor if there was already a graph for that id
      status               <- if (alreadyCreated) release.as(failure) else success[F]
    } yield status

  override def destroyGraph(req: DestroyGraph): F[Status] =
    for {
      graphOption <- if (req.force) forcedRemoval(req.graphId) else safeRemoval(req.graphId, req.clientId)
      status      <- graphOption.map(_.release.as(success)).getOrElse(failure[F]) // release graph if exists
    } yield status

  override def disconnectClient(req: ClientGraphId): F[Status] =
    graphs.modify { graphs =>
      val updatedGraphs = removeClientFromGraphs(graphs, req.graphId, req.clientId)
      val status        = if (graphs.isDefinedAt(req.graphId)) success else failure
      (updatedGraphs, status)
    }

  override def ingestData(request: protocol.IngestData): F[Status] =
    request match {
      case protocol.IngestData(TryIngestData(scala.util.Success(ingestData)), _) =>
        for {
          graph  <- graphs.get.map(graphs => graphs.get(ingestData.graphID))
          status <- executeIfGraphDefined(ingestData, graph)
        } yield status
      case _                                                                     => failure[F]
    }

  private def executeIfGraphDefined(ingestData: IngestData, graph: Option[Graph[F]]) =
    graph.map(graph => spawnExecutor(ingestData, graph).as(success)).getOrElse(failure[F])

  private def spawnExecutor(ingestData: IngestData, graph: Graph[F]) =
    graph.supervisor.supervise(processSource(ingestData).onError(_ => graph.release))

  private def processSource(req: IngestData): F[Unit] =
    req match {
      case IngestData(_, graphID, sourceId, source, blocking) =>
        IngestionExecutor(graphID, source, blocking, sourceId, config, repo)
    }

  private def forcedRemoval(graphId: String): F[Option[Graph[F]]] =
    graphs.modify(graphs => (graphs.removed(graphId), graphs.get(graphId)))

  private def safeRemoval(graphId: String, clientId: String): F[Option[Graph[F]]] =
    graphs.modify { graphs =>
      val graphsWithClientRemoved        = removeClientFromGraphs(graphs, graphId, clientId)
      lazy val graphsWithGraphRemoved    = graphs - graphId
      val (updatedGraph, graphToRelease) = graphsWithClientRemoved.get(graphId) match {
        case Some(graph) =>
          if (graph.clients.isEmpty) (graphsWithGraphRemoved, Some(graph)) else (graphsWithClientRemoved, None)
        case None        => (graphs, None)
      }
      (updatedGraph, graphToRelease)
    }

  private def removeClientFromGraphs(graphs: Map[String, Graph[F]], graphId: String, clientId: String) =
    graphs map {
      case (`graphId`, graph) => graphId -> graph.copy(clients = graph.clients.filterNot(_ == clientId))
      case tuple              => tuple
    }

  /** Removes the graph from the list of graphs and releases the supervisor of that graph */
  private def releaseGraph(graphId: String, supervisorRelease: F[Unit]) =
    for {
      graphExists <- graphs.modify { graphs =>
                       val graphExists = graphs.isDefinedAt(graphId)
                       (graphs - graphId, graphExists)
                     }
      _           <- if (graphExists) supervisorRelease else Async[F].unit
    } yield ()
}
