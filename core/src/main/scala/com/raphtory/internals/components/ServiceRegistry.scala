package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.ingestion.IngestionServiceImpl
import com.raphtory.internals.components.partition.PartitionServiceImpl
import com.raphtory.internals.components.querymanager.QueryServiceImpl
import com.raphtory.internals.management.Partitioner
import com.raphtory.protocol._
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.PartitionService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class ServiceRegistry[F[_]: Async](conf: Config) {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val partitioner = Partitioner(conf)

  private val partitionIds = 0 until partitioner.totalPartitions

  final def registered[T](instance: T, descriptor: ServiceDescriptor[F, T]): Resource[F, Int] =
    registered(instance, descriptor, List(0))

  final def registered[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): Resource[F, Int] =
    registered(instance, descriptor, List(id))

  /** Returns a resource containing the id allocated for the instance of the service */
  final def registered[T](instance: T, descriptor: ServiceDescriptor[F, T], candidateIds: List[Int]): Resource[F, Int] =
    Resource.apply {
      def firstSuccess(attempts: Seq[(Int, F[F[Unit]])]): F[(Int, F[Unit])] =
        attempts match {
          case (id, register) :: tail =>
            for {
              _      <- Async[F].delay(logger.debug(s"Trying to register $instance for id '$id' among $candidateIds"))
              result <- register.map(unregister => (id, unregister)).recoverWith(_ => firstSuccess(tail))
            } yield result

          case _                      =>
            val errorMsg =
              s"Failed to retrieve id among $candidateIds for instance of service ${descriptor.name}"
            Async[F].raiseError(new IllegalStateException(errorMsg))
        }

      val attempts = candidateIds.map(id => (id, register(instance, descriptor, id)))
      firstSuccess(attempts)
    }

  final def ingestion: Resource[F, IngestionService[F]] =
    getService[IngestionService[F]](IngestionServiceImpl.descriptor)

  final def partitions: Resource[F, Map[Int, PartitionService[F]]] =
    getServices(PartitionServiceImpl.descriptor, partitionIds)

  final def query: Resource[F, QueryService[F]] = getService[QueryService[F]](QueryServiceImpl.descriptor)

  private def getServices[T](descriptor: ServiceDescriptor[F, T], ids: Seq[Int]): Resource[F, Map[Int, T]] =
    ids
      .foldLeft(Resource.pure[F, Seq[T]](Seq()))((seq, id) => addServiceToSeq(seq, descriptor, id))
      .map(services => (ids zip services).toMap)

  private def addServiceToSeq[T](
      seq: Resource[F, Seq[T]],
      descriptor: ServiceDescriptor[F, T],
      id: Int
  ): Resource[F, Seq[T]] =
    seq flatMap (seq => getService(descriptor, id).map(service => seq :+ service))

  /** Register the instance and returns an IO to unregister it */
  protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]]
  protected def getService[T](descriptor: ServiceDescriptor[F, T], id: Int = 0): Resource[F, T]
}
