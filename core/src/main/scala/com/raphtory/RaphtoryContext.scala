package com.raphtory

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.confBuilder
import com.raphtory.Raphtory.shutdown
import com.raphtory.api.analysis.graphview.TemporalGraphConnection
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.repositories.PulsarTopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.Py4JServer
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.spouts.IdentitySpout
import com.typesafe.config.Config

import java.io.Closeable
import scala.reflect.ClassTag

/**
  * Fully initialised context that depends on an already running
  * deployment, either remote or local
  *
  * This offers an un-managed API to be used from Python where cats-effect makes little sense
  *
  * @param topicRepository
  * @param config
  */
class RaphtoryContext(topicRepository: TopicRepository, config: Config, shutdown: IO[Unit]) extends Closeable {
  override def close(): Unit = shutdown.unsafeRunSync()
}

object RaphtoryContext {}
