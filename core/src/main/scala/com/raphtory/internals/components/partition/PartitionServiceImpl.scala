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
import com.raphtory.internals.management.{Partitioner, Scheduler}
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
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

abstract class PartitionServiceImpl[F[_]: Async](
    id: Deferred[F, Int],
    graphs: GraphList[F, GraphPartition],
    topics: TopicRepository,
    config: Config
) extends OrchestratorService(graphs)
        with PartitionService[F] {

  override protected def graphExecution(graph: Graph[F, GraphPartition]): F[Unit] =
    (for {
      id <- Resource.eval(id.get)
      _  <- Writer[F](graph.id, id, graph.data, config, topics, new Scheduler)
    } yield ()).use(_ => Async[F].never)

  override def establishExecutor(req: Query): F[Status] =
    req match {
      case Query(TryQuery(Success(query)), unknownFields) => queryExecutor(query).as(success)
      case _                                              => failure[F]
    }

  private def queryExecutor(query: querymanager.Query) =
    for {
      id <- id.get
      _  <- attachExecutionToGraph(
                    query.graphID,
                    graph => QueryExecutor(query, id, graph.data, config, topics, new Scheduler)
            )
    } yield ()
}

class PojoPartitionServerImpl[F[_]: Async](
    id: Deferred[F, Int],
    graphs: GraphList[F, GraphPartition],
    topics: TopicRepository,
    config: Config
) extends PartitionServiceImpl[F](id, graphs, topics, config) {

  override def makeGraphData(graphId: String): F[GraphPartition] =
    id.get.map(id => new PojoBasedPartition(graphId, id, config))
}

class ArrowPartitionServerImpl[F[_]: Async, V: VertexSchema, E: EdgeSchema](
    id: Deferred[F, Int],
    graphs: GraphList[F, GraphPartition],
    topics: TopicRepository,
    config: Config
) extends PartitionServiceImpl[F](id, graphs, topics, config) {

  override def makeGraphData(graphId: String): F[GraphPartition] =
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

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def makeN[F[_]: Async](
      repo: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage((id, graphs) => new PojoPartitionServerImpl[F](id, graphs, repo.topics, config), repo, config)

  def makeNArrow[F[_]: Async, V: VertexSchema, E: EdgeSchema](
      repo: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage(
            (id, graphs) => new ArrowPartitionServerImpl[F, V, E](id, graphs, repo.topics, config),
            repo,
            config
    )

  def descriptor[F[_]: Async]: ServiceDescriptor[F, PartitionService[F]] =
    GrpcServiceDescriptor[F, PartitionService[F]](
            "partition",
            PartitionService.client(_),
            PartitionService.bindService[F](Async[F], _)
    )

  private def makeNWithStorage[F[_]: Async](
      service: (Deferred[F, Int], GraphList[F, GraphPartition]) => PartitionServiceImpl[F],
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
      service: (Deferred[F, Int], GraphList[F, GraphPartition]) => PartitionServiceImpl[F],
      repo: ServiceRegistry[F],
      conf: Config
  ): Resource[F, Unit] =
    for {
      graphs   <- makeGraphList[F, GraphPartition]
      id       <- Resource.eval(Deferred[F, Int])
      service  <- Resource.eval(Async[F].delay(service(id, graphs)))
      idNumber <- repo.registered(service, PartitionServiceImpl.descriptor, candidateIds.toList)
      _        <- Resource.eval(id.complete(idNumber))
      _        <- Resource.eval(id.get.map(id => logger.info(s"Starting partition service for id '$id'")))
    } yield ()
}
