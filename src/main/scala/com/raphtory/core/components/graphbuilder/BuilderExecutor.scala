package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.Component
import com.raphtory.core.config.AsyncConsumer
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.rits.cloning.Cloner
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

class BuilderExecutor[R: TypeTag: ClassTag](
    graphBuilder: GraphBuilder[R],
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[GraphUpdate, R](conf, pulsarController, scheduler) {
  private val safegraphBuilder  = new Cloner().deepClone(graphBuilder)
  private val producers         = toWriterProducers
  private val batches           = mutable.Map[Int, ArrayBuffer[GraphUpdate]]()
  private var totalMessagesSent = 0
  override val consumer         = Some(startGraphBuilderConsumer())

  override def run(): Unit = {
    logger.debug("Starting Graph Builder executor.")
    scheduler.execute(AsyncConsumer[GraphUpdate, R](this))
  }

  override def stop(): Unit = {
    logger.debug("Stopping Graph Builder executor.")

    consumer match {
      case Some(value) =>
        value.close()
    }
    producers.foreach(_._2.close())
  }

  override def handleMessage(msg: R): Boolean = {
    val data = msg
    safegraphBuilder.getUpdates(data).foreach(message => sendUpdate(message))
    true
  }

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")
    totalMessagesSent += 1
    if (totalMessagesSent % 100000 != 0)
      batches.get(getWriter(graphUpdate.srcId)) match {
        case Some(array) => array += graphUpdate
        case None        =>
          batches put (getWriter(graphUpdate.srcId), ArrayBuffer[GraphUpdate](graphUpdate))
      }
    else {
      logger.debug(s"Graph Builder has sent $totalMessagesSent")
      batches.foreach {
        case (partition, array) =>
          sendBatch(producers(getWriter(partition)), array.toArray)
          batches put (partition, ArrayBuffer[GraphUpdate]())
      }
    }

    // TODO Make into task
    sendMessage(producers(getWriter(graphUpdate.srcId)), graphUpdate)
  }

  override def name(): String = "Graph Builder"
}
