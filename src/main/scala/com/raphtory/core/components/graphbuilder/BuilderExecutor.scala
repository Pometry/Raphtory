package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.Component
import com.raphtory.core.config.{AsyncConsumer, MonixScheduler, PulsarController}
import com.rits.cloning.Cloner
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

class BuilderExecutor[T](
    schema: Schema[T],
    graphBuilder: GraphBuilder[T],
    conf: Config,
    pulsarController: PulsarController
) extends Component[T](conf, pulsarController) {
  private val safegraphBuilder = new Cloner().deepClone(graphBuilder)
  private val producers        = toWriterProducers

  override val cancelableConsumer  = Some(startGraphBuilderConsumer(schema))
  private val monixScheduler = new MonixScheduler

  override def run(): Unit = {
    logger.debug("Starting Graph Builder executor.")
    monixScheduler.scheduler.execute(AsyncConsumer(this))
  }

  override def getScheduler(): Scheduler = monixScheduler.scheduler

  override def stop(): Unit = {
    logger.debug("Stopping Graph Builder executor.")

    cancelableConsumer match {
      case Some(value) =>
        value.close()
    }
    producers.foreach(_._2.close())
  }

  override def handleMessage(msg: Message[T]): Boolean = {
    val data = msg.getValue
    safegraphBuilder.getUpdates(data).foreach(message => sendUpdate(message))
    true
  }

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")
    producers(getWriter(graphUpdate.srcId)).sendAsync(graphUpdate)

  }
}
