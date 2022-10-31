package com.raphtory.internals.components.ingestion

import cats.effect.Async
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.protocol.IngestionService

object IngestionServiceDescriptor {

  def apply[F[_]: Async]: ServiceDescriptor[F, IngestionService[F]] =
    GrpcServiceDescriptor[F, IngestionService[F]](
            "ingestion",
            IngestionService.client(_),
            IngestionService.bindService(Async[F], _)
    )
}
