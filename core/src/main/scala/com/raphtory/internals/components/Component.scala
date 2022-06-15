package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.kernel.Fiber
import cats.effect.kernel.Outcome.Succeeded
import cats.effect.kernel.Outcome.canceled
import com.raphtory.internals.communication.CancelableListener
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.Topic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.typesafe.config.Config

abstract private[raphtory] class Component[T](conf: Config) {

  protected val telemetry: ComponentTelemetryHandler.type = ComponentTelemetryHandler
  private val partitionServers: Int                       = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int                    = conf.getInt("raphtory.partitions.countPerServer")
  protected val totalPartitions: Int                      = partitionServers * partitionsPerServer
  val deploymentID: String                                = conf.getString("raphtory.deploy.id")

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt
  def handleMessage(msg: T): Unit
  private[raphtory] def run(): Unit
  private[raphtory] def stop(): Unit = {}
}

object Component {

  import cats.effect.syntax.spawn._
  import cats.syntax.all._

  def makeAndStart[IO[_]: Spawn, T, C <: Component[T]](
      repo: TopicRepository,
      name: String,
      ts: Seq[CanonicalTopic[T]],
      comp: => C
  )(implicit IO: Async[IO]): Resource[IO, C] =
    Resource
      .make {
        for {
          qm          <- IO.delay(comp)
          runnerFib   <- IO.blocking(qm.run()).start
          listener    <- IO.delay(repo.registerListener(s"${qm.deploymentID}-$name", qm.handleMessage, ts))
          listenerFib <- IO.blocking(listener.start()).start
        } yield (qm, listener, listenerFib, runnerFib)

      } {
        case (qm, listener, listenerFib, runner) =>
          cleanup(qm, listener, listenerFib, runner)
      }
      .map { case (qm, _, _, _) => qm }

  def makeAndStartPart[IO[_]: Spawn, T, C <: Component[T]](
      partitionId: Int,
      repo: TopicRepository,
      name: String,
      ts: Seq[Topic[T]],
      comp: => C
  )(implicit IO: Async[IO]): Resource[IO, C] =
    Resource
      .make {
        for {
          qm          <- IO.delay(comp)
          runnerFib   <- IO.blocking(qm.run()).start
          listener    <- IO.delay(repo.registerListener(s"${qm.deploymentID}-$name", qm.handleMessage, ts, partitionId))
          listenerFib <- IO.blocking(listener.start()).start
        } yield (qm, listener, listenerFib, runnerFib)
      } {
        case (qm, listener, listenerFib, runner) =>
          cleanup(qm, listener, listenerFib, runner)
      }
      .map { case (qm, _, _, _) => qm }

  private def cleanup[A, IO[_]](
      qm: Component[A],
      listener: CancelableListener,
      listenerFib: Fiber[IO, Throwable, Unit],
      runner: Fiber[IO, Throwable, Unit]
  )(implicit IO: Async[IO])                  =
    for {
      _ <- IO.blocking(listener.close())
      _ <- listenerFib.cancel
      _ <- IO.blocking(qm.stop())
      _ <- runner.cancel
    } yield ()
}
