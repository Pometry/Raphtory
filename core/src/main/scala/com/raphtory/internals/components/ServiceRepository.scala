package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.RaphtoryServiceBuilder.port
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.components.ingestion.IngestionOrchestratorBuilder
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import higherkindness.mu.rpc.ChannelForAddress
import higherkindness.mu.rpc.server.AddService
import higherkindness.mu.rpc.server.GrpcServer

case class ServiceRepository[F[_]](ingestion: IngestionOrchestrator[F], repo: TopicRepository)

object ServiceRepository {

  def apply[F[_]: Async](repo: TopicRepository, config: Config): Resource[F, ServiceRepository[F]] =
    for {
      ingestion <- IngestionOrchestratorBuilder[F](repo, config)
    } yield ServiceRepository(ingestion = ingestion, repo = repo)
}
