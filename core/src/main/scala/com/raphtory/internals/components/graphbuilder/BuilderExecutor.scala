package com.raphtory.internals.components.graphbuilder

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.GraphUpdate
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
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

  private val spoutOutputListener =
    topics.registerListener(s"$deploymentID-builder-$name", handleMessage, topics.spout[T])

  private var messagesProcessed = 0

  override def run(): Unit = {
    logger.debug(
            s"Starting Graph Builder executor with deploymentID ${conf.getString("raphtory.deploy.id")}"
    )
    spoutOutputListener.start()
  }

  override def stop(): Unit = {
    logger.debug("Stopping Graph Builder executor.")
    spoutOutputListener.close()
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
