package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.OrchestratorService
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.components.querymanager
import com.raphtory.internals.components.querymanager.TryQuery
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.raphtory.protocol.Empty
import com.raphtory.protocol.GraphAlteration
import com.raphtory.protocol.GraphAlterations
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.Query
import com.raphtory.protocol.Status
import com.raphtory.protocol.failure
import com.raphtory.protocol.success
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import scala.util.Success
import PartitionServiceImpl.Partition

abstract class PartitionServiceImpl[F[_]: Async](
    id: Deferred[F, Int],
    graphs: GraphList[F, Partition[F]],
    registry: ServiceRegistry[F],
    config: Config
) extends OrchestratorService(graphs)
        with PartitionService[F] {

  protected def makeGraphStorage(graphId: String): F[GraphPartition]

  override protected def makeGraphData(graphId: String): Resource[F, Partition[F]] =
    for {
      id      <- Resource.eval(id.get)
      storage <- Resource.eval(makeGraphStorage(graphId))
      writer  <- Writer(graphId, id, storage, registry, config)
    } yield Partition(storage, writer)

  override protected def graphExecution(graph: Graph[F, Partition[F]]): F[Unit] =
    (for {
      id <- Resource.eval(id.get)
      _  <- Reader[F](graph.id, id, graph.data.storage, new Scheduler, config, registry.topics)
    } yield ()).use(_ => Async[F].never)

  override def establishExecutor(req: Query): F[Status] =
    req match {
      case Query(TryQuery(Success(query)), unknownFields) => queryExecutor(query).as(success)
      case _                                              => failure[F]
    }

  override def processUpdates(req: GraphAlterations): F[Empty] =
    for {
      writer <- graphs.get.map(graphs => graphs(req.graphId).data.writer)
      _      <- writer.processUpdates(req)
    } yield Empty()

  override def processEffects(req: GraphAlterations): F[Empty] =
    for {
      writer <- graphs.get.map(graphs => graphs(req.graphId).data.writer)
      _      <- writer.processEffects(req)
    } yield Empty()

  private def queryExecutor(query: querymanager.Query) =
    for {
      id <- id.get
      _  <- attachExecutionToGraph(
                    query.graphID,
                    graph => QueryExecutor(query, id, graph.data.storage, config, registry.topics, new Scheduler)
            )
    } yield ()
}

class PojoPartitionServerImpl[F[_]: Async](
    id: Deferred[F, Int],
    graphs: GraphList[F, Partition[F]],
    registry: ServiceRegistry[F],
    config: Config
) extends PartitionServiceImpl[F](id, graphs, registry, config) {

  override protected def makeGraphStorage(graphId: String): F[GraphPartition] =
    id.get.map(id => new PojoBasedPartition(graphId, id, config))
}

class ArrowPartitionServerImpl[F[_]: Async, V: VertexSchema, E: EdgeSchema](
    id: Deferred[F, Int],
    graphs: GraphList[F, Partition[F]],
    registry: ServiceRegistry[F],
    config: Config
) extends PartitionServiceImpl[F](id, graphs, registry, config) {

  override protected def makeGraphStorage(graphId: String): F[GraphPartition] =
    for {
      id      <- id.get
      storage <-
        Async[F].blocking(
                ArrowPartition(
                        graphId,
                        ArrowPartitionConfig(config, id, ArrowSchema[V, E], Files.createTempDirectory("experimental")),
                        config
                )
        )
    } yield storage
}

object PartitionServiceImpl {
  import OrchestratorService._

  case class Partition[F[_]](storage: GraphPartition, writer: Writer[F])

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def makeN[F[_]: Async](
      registry: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage((id, graphs) => new PojoPartitionServerImpl[F](id, graphs, registry, config), registry, config)

  def makeNArrow[F[_]: Async, V: VertexSchema, E: EdgeSchema](
      registry: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage(
            (id, graphs) => new ArrowPartitionServerImpl[F, V, E](id, graphs, registry, config),
            registry,
            config
    )

  def descriptor[F[_]: Async]: ServiceDescriptor[F, PartitionService[F]] =
    GrpcServiceDescriptor[F, PartitionService[F]](
            "partition",
            PartitionService.client(_),
            PartitionService.bindService[F](Async[F], _)
    )

  private def makeNWithStorage[F[_]: Async](
      service: (Deferred[F, Int], GraphList[F, Partition[F]]) => PartitionServiceImpl[F],
      repo: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] = {
    val partitioner      = Partitioner()
    val candidateIds     = 0 until partitioner.totalPartitions
    val partitionsToMake = 0 until partitioner.partitionsPerServer
    partitionsToMake
      .map(_ => makePartition(candidateIds, service, repo, config))
      .reduce((p1, p2) => p1 flatMap (_ => p2))
  }

  private def makePartition[F[_]: Async](
      candidateIds: Seq[Int],
      service: (Deferred[F, Int], GraphList[F, Partition[F]]) => PartitionServiceImpl[F],
      repo: ServiceRegistry[F],
      conf: Config
  ): Resource[F, Unit] =
    for {
      graphs   <- makeGraphList[F, Partition[F]]
      id       <- Resource.eval(Deferred[F, Int])
      service  <- Resource.eval(Async[F].delay(service(id, graphs)))
      idNumber <- repo.registered(service, PartitionServiceImpl.descriptor, candidateIds.toList)
      _        <- Resource.eval(id.complete(idNumber))
      _        <- Resource.eval(id.get.map(id => logger.info(s"Starting partition service for id '$id'")))
    } yield ()
}
