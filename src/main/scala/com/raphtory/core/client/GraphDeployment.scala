package com.raphtory.core.client

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.ThreadedWorker
import com.raphtory.core.config.ZookeeperIDManager
import com.typesafe.config.Config
import monix.execution.Scheduler

import scala.language.postfixOps
import scala.reflect.ClassTag

/**
  * {s}`RaphtoryGraph`
  *    : Raphtory Graph extends Raphtory Client to initialise Query Manager, Partitions, Spout Worker
  *    and GraphBuilder Worker for a deployment ID
  *
  *  {s}`RaphtoryGraph` should not be created directly. To create a {s}`RaphtoryGraph` use
  *  {s}`Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.
  *
  *  The query methods for `RaphtoryGraph` are similar to `RaphtoryClient`
  *
  *  ## Methods
  *
  *    {s}`stop()`
  *      : Stops components - partitions, query manager, graph builders, spout worker
  *
  *  ```{seealso}
  *  [](com.raphtory.core.client.RaphtoryClient), [](com.raphtory.core.deploy.Raphtory)
  *  ```
  */
private[core] class GraphDeployment[T: ClassTag](
    spout: SpoutExecutor[T],
    graphBuilder: GraphBuilder[T],
    private val queryBuilder: QueryBuilder,
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler
) extends RaphtoryClient(
                queryBuilder,
                conf
        ) {

  allowIllegalReflection()

  private val deploymentID: String = conf.getString("raphtory.deploy.id")
  private val spoutTopic: String   = conf.getString("raphtory.spout.topic")

  private val zookeeperAddress: String = conf.getString("raphtory.zookeeper.address")

  private val partitionIdManager =
    new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")
  partitionIdManager.resetID()

  private val builderIdManager =
    new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/builderCount")
  builderIdManager.resetID()

  private val partitions                     = componentFactory.partition(scheduler)
  private val queryManager                   = componentFactory.query(scheduler)
  private val spoutworker: ThreadedWorker[T] = componentFactory.spout(spout, scheduler)

  private val graphBuilderworker: List[ThreadedWorker[T]] =
    componentFactory.builder[T](graphBuilder, scheduler)

  logger.info(s"Created Graph object with deployment ID '$deploymentID'.")
  logger.info(s"Created Graph Spout topic with name '$spoutTopic'.")

  def stop(): Unit = {
    partitions.foreach { partition =>
      partition.writer.stop()
      partition.reader.stop()
    }
    queryManager.worker.stop()
    spoutworker.worker.stop()
    graphBuilderworker.foreach(builder => builder.worker.stop())
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
