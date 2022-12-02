package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.components.partition.PartitionServiceImpl
import com.raphtory.internals.components.registries._
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus

object Partition extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      _    <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      repo <- DistributedServiceRegistry[IO](config)
      _    <- PartitionServiceImpl.makeN(repo, config)
    } yield ()
  }
}
