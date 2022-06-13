package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.kernel.Outcome.Succeeded
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

  def makeAndStartSimple[IO[_]: Spawn, SP, C <: Component[SP]](
      comp: => C
  )(implicit IO: Async[IO]): Resource[IO, C] =
    Resource
      .make {
        for {
          sp    <- IO.delay(comp)
          spFib <- IO.delay(sp.run()).start
        } yield (sp, spFib)
      } {
        case (sp, spFib) => spFib.cancel *> IO.delay(sp.stop())
      } // FIXME: review if this is sane, if the FIRST while loop didn't finish it will be canceled
      .map { case (sp, _) => sp }

  def makeAndStart[IO[_], T, C <: Component[T]](
      repo: TopicRepository,
      name: String,
      ts: Seq[CanonicalTopic[T]],
      comp: => C
  )(implicit
      IO: Async[IO],
      C: Spawn[IO]
  ): Resource[IO, C]                         =
    Resource
      .make {
        for {
          qm       <- IO.delay(comp)
          listener <- IO.delay {
                        repo.registerListener(
                                s"${qm.deploymentID}-$name",
                                qm.handleMessage,
                                ts
                        )
                      }.start
        } yield (qm, listener)
      } {
        case (qm, listenerFib) =>
          for {
            listener <- listenerFib.join
            _        <- IO.defer {
                          listener match {
                            case Succeeded(fib) => fib.map(_.close())
                            case _              => IO.delay(println(s"Unable to close $name listener"))
                          }
                        }
            _        <- IO.delay(qm.stop())
          } yield ()
      }
      .map { case (qm, _) => qm }

  def makeAndStartPart[IO[_], T, C <: Component[T]](
      partitionId: Int,
      repo: TopicRepository,
      name: String,
      ts: Seq[Topic[T]],
      comp: => C
  )(implicit
      IO: Async[IO],
      C: Spawn[IO]
  ): Resource[IO, C]                         =
    Resource
      .make {
        for {
          qm       <- IO.delay(comp)
          listener <- IO.delay {
                        repo.registerListener(
                                s"${qm.deploymentID}-$name",
                                qm.handleMessage,
                                ts,
                                partitionId
                        )
                      }.start
        } yield (qm, listener)
      } {
        case (qm, listenerFib) =>
          for {
            listener <- listenerFib.join
            _        <- IO.defer {
                          listener match {
                            case Succeeded(fib) => fib.map(_.close())
                            case _              => IO.delay(println(s"Unable to close $name listener"))
                          }
                        }
            _        <- IO.delay(qm.stop())
          } yield ()
      }
      .map { case (qm, _) => qm }
}
