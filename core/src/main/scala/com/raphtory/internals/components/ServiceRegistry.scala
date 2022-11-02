package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceImpl
import com.raphtory.internals.components.partition.PartitionServiceImpl
import com.raphtory.internals.management.Partitioner
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.PartitionService
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class ServiceRegistry[F[_]: Async](val topics: TopicRepository) {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val partitioner = Partitioner()

  /** Returns a resource containing the id allocated for the instance of the service */
  final def registered[T](
      instance: T,
      descriptor: ServiceDescriptor[F, T],
      candidateIds: List[Int] = List(0)
  ): Resource[F, Int] =
    Resource.apply {
      def firstSuccess(attempts: Seq[(Int, F[F[Unit]])]): F[(Int, F[Unit])] =
        attempts match {
          case (id, register) :: tail =>
            for {
              _      <-
                Async[F].delay(logger.debug(s"Trying to register $instance for id '$id' among ${candidateIds.toList}"))
              result <- register.map(unregister => (id, unregister)).recoverWith(_ => firstSuccess(tail))
            } yield result

          case _                      =>
            val errorMsg =
              s"Failed to retrieve id among ${candidateIds.toList} for instance of service ${descriptor.name}"
            Async[F].raiseError(new IllegalStateException(errorMsg))
        }

      val attempts = candidateIds.map(id => (id, register(instance, descriptor, id)))
      firstSuccess(attempts)
    }

  final def ingestion: Resource[F, IngestionService[F]] =
    getService[IngestionService[F]](IngestionServiceImpl.descriptor)

  final def partitions: Resource[F, Seq[PartitionService[F]]] =
    getServices(PartitionServiceImpl.descriptor, 0 until partitioner.totalPartitions)

  private def getServices[T](descriptor: ServiceDescriptor[F, T], ids: Seq[Int]): Resource[F, Seq[T]] =
    ids.foldLeft(Resource.pure[F, Seq[T]](Seq()))((seq, id) => addServiceToSeq(seq, descriptor, id))

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
