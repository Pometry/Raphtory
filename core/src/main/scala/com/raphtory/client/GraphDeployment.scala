package com.raphtory.client

import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.config.ComponentFactory
import com.raphtory.config.Partitions
import com.raphtory.config.ThreadedWorker
import com.raphtory.config.ZookeeperIDManager
import com.typesafe.config.Config
import monix.execution.Scheduler
import io.prometheus.client.exporter.HTTPServer

import java.io.IOException
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * {s}`GraphDeployment`
  *    : Graph Deployment extends Raphtory Client to initialise Query Manager, Partitions, Spout Worker
  *    and GraphBuilder Worker for a deployment ID
  *
  *  {s}`GraphDeployment` should not be created directly. To create a {s}`GraphDeployment` use
  *  {s}`Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.
  *
  *  The query methods for `GraphDeployment` are similar to `RaphtoryClient`
  *
  *  ## Methods
  *
  *    {s}`stop()`
  *      : Stops components - partitions, query manager, graph builders, spout worker
  *
  *  ```{seealso}
  *  [](com.raphtory.client.RaphtoryClient), [](com.raphtory.deployment.Raphtory)
  *  ```
  */
private[raphtory] class GraphDeployment[T: ClassTag: TypeTag](
    batchLoading: Boolean,
    spout: Spout[T],
    graphBuilder: GraphBuilder[T],
    private val querySender: QuerySender,
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler
) extends RaphtoryClient(
                querySender,
                conf
        ) {

  allowIllegalReflection()

  private val deploymentID: String = conf.getString("raphtory.deploy.id")
  private val spoutTopic: String   = conf.getString("raphtory.spout.topic")
  private val prometheusPort: Int  = conf.getInt("raphtory.prometheus.server")

  private val partitions: Partitions =
    componentFactory.partition(scheduler, batchLoading, Some(spout), Some(graphBuilder))

  private val queryManager = componentFactory.query(scheduler)

  private val spoutworker: Option[ThreadedWorker[T]] =
    componentFactory.spout(spout, batchLoading, scheduler)

  private val graphBuilderworker: Option[List[ThreadedWorker[T]]] =
    componentFactory.builder[T](graphBuilder, batchLoading, scheduler)

  private var prometheusServer: Option[HTTPServer] = None

  logger.info(s"Created Graph object with deployment ID '$deploymentID'.")
  logger.info(s"Created Graph Spout topic with name '$spoutTopic'.")

  try prometheusServer = Option(new HTTPServer(prometheusPort))
  catch {
    case e: IOException => e.printStackTrace()
  }

  def stop(): Unit = {
    partitions.writers.foreach(_.stop())
    partitions.readers.foreach(_.stop())
    queryManager.worker.stop()

    spoutworker match {
      case Some(w) => w.worker.stop()
      case None    =>
    }
    graphBuilderworker match {
      case Some(worker) => worker.foreach(builder => builder.worker.stop())
      case None         =>
    }

    prometheusServer match {
      case Some(w) => w.stop()
      case None    =>
    }

    componentFactory.stop()
  }

  private def allowIllegalReflection() = {
    import java.lang.reflect.Field

    try { // Turn off illegal access log messages.
      val loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger")
      val loggerField = loggerClass.getDeclaredField("logger")
      val unsafeClass = Class.forName("sun.misc.Unsafe")
      val unsafeField = unsafeClass.getDeclaredField("theUnsafe")
      unsafeField.setAccessible(true)
      val unsafe      = unsafeField.get(null)
      val offset      =
        unsafeClass
          .getMethod("staticFieldOffset", classOf[Field])
          .invoke(unsafe, loggerField)
          .asInstanceOf[Long]
      unsafeClass
        .getMethod("putObjectVolatile", classOf[Object], classOf[Long], classOf[Object])
        .invoke(unsafe, loggerClass, offset, null)
    }
    catch {
      case ex: Exception =>
        logger.warn("Failed to disable Java 10 access warning:")
        ex.printStackTrace()
    }
  }
}
