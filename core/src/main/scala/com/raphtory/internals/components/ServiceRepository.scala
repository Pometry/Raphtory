package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceInstance
import com.raphtory.protocol.IngestionService

abstract class ServiceRepository[F[_]: Async](val topics: TopicRepository) {
  def registered[T](instance: T, descriptor: ServiceDescriptor[F, T]): Resource[F, Unit]
  protected def getService[T](descriptor: ServiceDescriptor[F, T]): Resource[F, T]

  final def ingestion: Resource[F, IngestionService[F]] =
    getService[IngestionService[F]](IngestionServiceInstance.descriptor)
}
