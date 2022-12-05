package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.OrchestratorService
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.components.querymanager
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.TryQuery
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.raphtory.protocol.GraphAlterations
import com.raphtory.protocol.NodeCount
import com.raphtory.protocol.Operation
import com.raphtory.protocol.OperationAndState
import com.raphtory.protocol.OperationResult
import com.raphtory.protocol.OperationWithStateResult
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.PerspectiveCommand
import com.raphtory.protocol.Query
import com.raphtory.protocol.QueryId
import com.raphtory.protocol.Status
import com.raphtory.protocol.VertexMessages
import com.raphtory.protocol.failure
import com.raphtory.protocol.success
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import scala.util.Success
import PartitionServiceImpl.Partition
import cats.effect.std.Dispatcher
import com.google.protobuf.empty.Empty

abstract class PartitionServiceImpl[F[_]](
    id: Deferred[F, Int],
    partitions: Deferred[F, Map[Int, PartitionService[F]]],
    graphs: GraphList[F, Partition[F]],
    dispatcher: Dispatcher[F],
    config: Config
)(implicit F: Async[F])
        extends OrchestratorService(graphs)
        with PartitionService[F] {

  protected def makeGraphStorage(graphId: String): F[GraphPartition]

  override protected def makeGraphData(graphId: String): Resource[F, Partition[F]] =
    for {
      id         <- Resource.eval(id.get)
      storage    <- Resource.eval(makeGraphStorage(graphId))
      partitions <- Resource.eval(partitions.get)
      writer     <- Writer[F](graphId, id, storage, partitions, config)
    } yield Partition(storage, writer, Map())

  override def establishExecutor(req: Query): F[Status] =
    req match {
      case Query(TryQuery(Success(query)), _) => queryExecutor(query).as(success)
      case _                                  => failure[F]
    }

  override def processUpdates(req: GraphAlterations): F[Empty] =
    for {
      writer <- graphs.get.map(graphs => graphs(req.graphId).data.writer)
      _      <- writer.processUpdates(req)
    } yield Empty()

//  override def processEffects(req: GraphAlterations): F[Empty] =
//    for {
//      writer <- graphs.get.map(graphs => graphs(req.graphId).data.writer)
//      _      <- writer.processEffects(req)
//    } yield Empty()

  override def receiveMessages(req: VertexMessages): F[Empty] =
    forwardToExecutor(
            req.graphId,
            req.jobId,
            _.receiveMessages(req.messages.asInstanceOf[Seq[GenericVertexMessage[_]]])
    )

  override def establishPerspective(req: PerspectiveCommand): F[NodeCount] =
    forwardToExecutor(req.graphId, req.jobId, _.establishPerspective(req.perspective))

  override def setMetadata(req: NodeCount): F[Empty] =
    forwardToExecutor(req.graphId, req.jobId, _.setMetadata(req.count))

  override def executeOperation(req: Operation): F[OperationResult] =
    forwardToExecutor(req.graphId, req.jobId, _.executeOperation(req.number))

  override def executeOperationWithState(req: OperationAndState): F[OperationWithStateResult] =
    forwardToExecutor(req.graphId, req.jobId, _.executeOperationWithState(req.number, req.state))

  override def getResult(req: QueryId): F[PartitionResult] = forwardToExecutor(req.graphId, req.jobId, _.getResult)

  override def writePerspective(req: PerspectiveCommand): F[Empty] =
    forwardToExecutor(req.graphId, req.jobId, _.writePerspective(req.perspective))

  override def endQuery(req: QueryId): F[Empty] =
    for {
      _ <- forwardToExecutor(req.graphId, req.jobId, _.endQuery)
      _ <- updateExecutorsForGraph(req.graphId, _ - req.jobId)
    } yield Empty()

  private def forwardToExecutor[T](graphId: String, jobId: String, f: QueryExecutor[F] => F[T]): F[T] =
    for {
      executor <- graphs.get.map(graphs => graphs(graphId).data.executors(jobId))
      result   <- f(executor)
    } yield result

  private def queryExecutor(query: querymanager.Query) =
    for {
      id         <- id.get
      storage    <- graphs.get.map(list => list(query.graphID).data.storage)
      partitions <- partitions.get
      executor   <- QueryExecutor[F](query, id, storage, partitions, dispatcher, config)
      _          <- updateExecutorsForGraph(query.graphID, _ + (query.name -> executor))
    } yield ()

  private def updateExecutorsForGraph(
      graphId: String,
      updating: Map[String, QueryExecutor[F]] => Map[String, QueryExecutor[F]]
  ) =
    graphs.update(graphs =>
      graphs.updatedWith(graphId)(_.map { graph =>
        val partition        = graph.data
        val executorsUpdated = updating(partition.executors)
        val partitionUpdated = partition.copy(executors = executorsUpdated)
        graph.copy(data = partitionUpdated)
      })
    )
}

class PojoPartitionServerImpl[F[_]: Async](
    id: Deferred[F, Int],
    partitions: Deferred[F, Map[Int, PartitionService[F]]],
    graphs: GraphList[F, Partition[F]],
    dispatcher: Dispatcher[F],
    config: Config
) extends PartitionServiceImpl[F](id, partitions, graphs, dispatcher, config) {

  override protected def makeGraphStorage(graphId: String): F[GraphPartition] =
    id.get.map(id => new PojoBasedPartition(graphId, id, config))
}

class ArrowPartitionServerImpl[F[_]: Async, V: VertexSchema, E: EdgeSchema](
    id: Deferred[F, Int],
    partitions: Deferred[F, Map[Int, PartitionService[F]]],
    graphs: GraphList[F, Partition[F]],
    dispatcher: Dispatcher[F],
    config: Config
) extends PartitionServiceImpl[F](id, partitions, graphs, dispatcher, config) {

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

  case class Partition[F[_]](storage: GraphPartition, writer: Writer[F], executors: Map[String, QueryExecutor[F]])

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def makeN[F[_]: Async](
      registry: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage(
            (id, partitions, graphs, dispatcher) =>
              new PojoPartitionServerImpl[F](id, partitions, graphs, dispatcher, config),
            registry,
            config
    )

  def makeNArrow[F[_]: Async, V: VertexSchema, E: EdgeSchema](
      registry: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] =
    makeNWithStorage(
            (id, partitions, graphs, dispatcher) =>
              new ArrowPartitionServerImpl[F, V, E](id, partitions, graphs, dispatcher, config),
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
      service: (
          Deferred[F, Int],
          Deferred[F, Map[Int, PartitionService[F]]],
          GraphList[F, Partition[F]],
          Dispatcher[F]
      ) => PartitionServiceImpl[F],
      registry: ServiceRegistry[F],
      config: Config
  ): Resource[F, Unit] = {
    val partitioner      = Partitioner()
    val candidateIds     = 0 until partitioner.totalPartitions
    val partitionsToMake = 0 until partitioner.partitionsPerServer
    partitionsToMake
      .map(_ => makePartition(candidateIds, service, registry, config))
      .reduce((p1, p2) => p1 flatMap (_ => p2))
  }

  private def makePartition[F[_]: Async](
      candidateIds: Seq[Int],
      service: (
          Deferred[F, Int],
          Deferred[F, Map[Int, PartitionService[F]]],
          GraphList[F, Partition[F]],
          Dispatcher[F]
      ) => PartitionServiceImpl[F],
      registry: ServiceRegistry[F],
      conf: Config
  ): Resource[F, Unit] =
    for {
      graphs        <- makeGraphList[F, Partition[F]]
      id            <- Resource.eval(Deferred[F, Int])
      partitionsDfr <- Resource.eval(Deferred[F, Map[Int, PartitionService[F]]])
      dispatcher    <- Dispatcher[F]
      service       <- Resource.eval(Async[F].delay(service(id, partitionsDfr, graphs, dispatcher)))
      idNumber      <- registry.registered(service, PartitionServiceImpl.descriptor, candidateIds.toList)
      _             <- Resource.eval(id.complete(idNumber))
      _             <- registry.partitions.evalMap(partitions => partitionsDfr.complete(partitions)).start
      _             <- Resource.eval(id.get.map(id => logger.info(s"Starting partition service for id '$id'")))
    } yield ()
}
