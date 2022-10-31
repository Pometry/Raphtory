package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceImpl
import com.raphtory.protocol.IngestionService

abstract class ServiceRepository[F[_]: Async](val topics: TopicRepository) {

  /** Returns a resource containing the id allocated for the instance of the service */
  final def registered[T](
      instance: T,
      descriptor: ServiceDescriptor[F, T],
      candidateIds: Seq[Int] = Seq(0)
  ): Resource[F, Int] =
    Resource.apply {
      def firstSuccess(attempts: Seq[(Int, F[F[Unit]])]): F[(Int, F[Unit])] =
        attempts match {
          case (id, register) :: tail =>
            register.map(unregister => (id, unregister)).recoverWith(_ => firstSuccess(tail))
          case _                      =>
            val errorMsg = s"Failed to retrieve id among $candidateIds for instance of service ${descriptor.name}"
            Async[F].raiseError(new Exception(errorMsg))
        }

      val attempts = candidateIds.map(id => (id, register(instance, descriptor, id)))
      firstSuccess(attempts)
    }

  final def ingestion: Resource[F, IngestionService[F]] =
    getService[IngestionService[F]](IngestionServiceImpl.descriptor)

  /** Register the instance and returns an IO to unregister it */
  protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]]
  protected def getService[T](descriptor: ServiceDescriptor[F, T], id: Int = 0): Resource[F, T]
}
