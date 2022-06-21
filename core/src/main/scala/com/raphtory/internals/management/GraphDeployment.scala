package com.raphtory.internals.management

import com.raphtory.api.analysis.graphview.Deployment
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.prometheus.client.exporter.HTTPServer
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Graph Deployment extends Deployment to initialise Query Manager, Partitions, Spout Worker and GraphBuilder Worker for a deployment ID
  *  `GraphDeployment` should not be created directly. To create a `GraphDeployment` use
  *  `Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.
  */
private[raphtory] class GraphDeployment[T: ClassTag: TypeTag](
    batchLoading: Boolean,
    spout: Spout[T],
    graphBuilder: GraphBuilder[T],
    val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler
) extends Deployment {

  allowIllegalReflection()
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val deploymentID: String = conf.getString("raphtory.deploy.id")
  private val spoutTopic: String   = conf.getString("raphtory.spout.topic")

  // private var partitions: Partitions =
  //   componentFactory.partition(scheduler, batchLoading, Some(spout), Some(graphBuilder))

//  private var queryManager = componentFactory.query(scheduler)

//  private var spoutworker: Option[ThreadedWorker[T]] =
//    componentFactory.spout(spout, batchLoading, scheduler)

//  private var graphBuilderworker: Option[List[ThreadedWorker[T]]] =
//    componentFactory.builder[T](graphBuilder, batchLoading, scheduler)

  logger.info(s"Created Graph object with deployment ID '$deploymentID'.")
  logger.info(s"Created Graph Spout topic with name '$spoutTopic'.")

  /** Stops components - partitions, query manager, graph builders, spout worker */
  def stop(): Unit = {}
  // partitions.writers.foreach(_.stop())
  // partitions.readers.foreach(_.stop())
  // partitions = null
//    queryManager.worker.stop()
//    queryManager = null

//    spoutworker match {
//      case Some(w) =>
//        w.worker.stop()
//        spoutworker = null
//      case None    =>
//    }
//    graphBuilderworker match {
//      case Some(worker) =>
//        worker.foreach(builder => builder.worker.stop())
//        graphBuilderworker = null
//
//      case None         =>
//    }
//    componentFactory.stop()

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
