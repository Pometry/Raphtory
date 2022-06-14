package com.raphtory.internals.components.graphbuilder

import cats.Foldable
import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.serialisers.Marshal
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

private[raphtory] class BuilderExecutor[T: ClassTag](
    name: Int,
    deploymentID: String,
    graphBuilder: GraphBuilder[T],
    conf: Config,
    topics: TopicRepository
) extends Component[T](conf) {
  private val safegraphBuilder     = Marshal.deepCopy(graphBuilder)
  safegraphBuilder
    .setBuilderMetaData(
            name,
            deploymentID
    )
  private val failOnError: Boolean = conf.getBoolean("raphtory.builders.failOnError")
  private val writers              = topics.graphUpdates.endPoint
  private val logger: Logger       = Logger(LoggerFactory.getLogger(this.getClass))

  private var messagesProcessed = 0

  override def run(): Unit =
    logger.debug(
            s"Starting Graph Builder executor with deploymentID ${conf.getString("raphtory.deploy.id")}"
    )

  override def stop(): Unit = {
    logger.debug("Stopping Graph Builder executor.")
    writers.values.foreach(_.close())
  }

  override def handleMessage(msg: T): Unit =
    safegraphBuilder
      .getUpdates(msg)(failOnError = failOnError)
      .foreach { message =>
        sendUpdate(message)
        telemetry.graphBuilderUpdatesCounter.labels(deploymentID).inc()
      }

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")

    writers(getWriter(graphUpdate.srcId)).sendAsync(graphUpdate)

    messagesProcessed = messagesProcessed + 1

    if (messagesProcessed % 100_000 == 0)
      logger.debug(s"Graph builder $name: sent $messagesProcessed messages.")
  }
}

object BuilderExecutor {

  def apply[IO[_]: Async: Spawn, T](
      name: Int,
      deploymentID: String,
      graphBuilder: GraphBuilder[T],
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, BuilderExecutor[T]] =
    Component.makeAndStart(
            topics,
            s"builder-executor-$name",
            List(topics.spout[T]),
            new BuilderExecutor[T](name, deploymentID, graphBuilder, conf, topics)
    )
}

// FIXME: probably good use of cats-effect Supervisor
class BuildExecutorGroup(config: Config)

object BuildExecutorGroup {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  import alleycats.std.iterable._

  def apply[IO[_]: Async: Spawn, T: ClassTag](
      config: Config,
      builderIDManager: IDManager,
      topics: TopicRepository,
      graphBuilder: GraphBuilder[T]
  ): Resource[IO, BuildExecutorGroup] = {
    val totalBuilders        = config.getInt("raphtory.builders.countPerServer")
    val deploymentID: String = config.getString("raphtory.deploy.id")

    logger.info(s"Creating '$totalBuilders' Graph Builders.")

    logger.debug(s"Deployment ID set to '$deploymentID'.")

    val iter = (0 until totalBuilders)
      .map { _ =>
        for {
          name <- Resource.eval(Async[IO].blocking {
                    builderIDManager
                      .getNextAvailableID()
                      .getOrElse(
                              throw new Exception(
                                      s"Failed to retrieve Builder ID. " +
                                        s"ID Manager at Zookeeper '$builderIDManager' was unreachable."
                              )
                      )
                  })
          _    <- BuilderExecutor(name, deploymentID, graphBuilder, config, topics)
        } yield ()
      }

    Foldable[Iterable]
      .sequence_(iter)
      .map(_ => new BuildExecutorGroup(config))

  }
}
