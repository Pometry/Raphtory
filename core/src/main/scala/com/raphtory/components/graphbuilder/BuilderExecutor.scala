package com.raphtory.components.graphbuilder

import com.raphtory.components.Component
import com.raphtory.config.PulsarController
import com.raphtory.config.TopicRepository
import com.raphtory.serialisers.Marshal
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/** @DoNotDocument */
class BuilderExecutor[T: ClassTag](
    name: String,
    graphBuilder: GraphBuilder[T],
    conf: Config,
    topics: TopicRepository
) extends Component[T](conf) {
  private val safegraphBuilder     = Marshal.deepCopy(graphBuilder)
  private val failOnError: Boolean = conf.getBoolean("raphtory.builders.failOnError")
  private val writers              = topics.graphUpdates.endPoint(update => getWriter(update.srcId))
  private val spoutOutputListener  = topics.registerListener(handleMessage, topics.spoutOutput[T])

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
    writers.close()
  }

  override def handleMessage(msg: T): Unit =
    safegraphBuilder
      .getUpdates(msg)(failOnError = failOnError)
      .foreach(message => sendUpdate(message))

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")

    writers.sendAsync(graphUpdate)
    //.thenApply(msgId => msgId -> null) TODO: remove I guess ?

    messagesProcessed = messagesProcessed + 1

    if (messagesProcessed % 100_000 == 0)
      logger.debug(s"Graph builder $name: sent $messagesProcessed messages.")
  }
}
