package com.raphtory.core.client

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.executor.IdentitySpoutExecutor
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.PulsarController
import com.raphtory.core.config.ThreadedWorker
import com.raphtory.core.config.ZookeeperIDManager
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

import scala.language.postfixOps
import scala.reflect.runtime.universe._

private[core] class RaphtoryGraph[T: TypeTag](
    spout: SpoutExecutor[T],
    graphBuilder: GraphBuilder[T],
    schema: Schema[T],
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController
) extends RaphtoryClient(
                conf.getString("raphtory.deploy.id"),
                conf,
                componentFactory,
                scheduler,
                pulsarController
        ) {

  allowIllegalReflection()

  private val deploymentID: String = conf.getString("raphtory.deploy.id")
  private val spoutTopic: String   = conf.getString("raphtory.spout.topic")

  private val zookeeperAddress: String = conf.getString("raphtory.zookeeper.address")
  private val idManager                = new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")
  idManager.resetID()

  private val partitions                     = componentFactory.partition(scheduler)
  private val queryManager                   = componentFactory.query(scheduler)
  private val spoutworker: ThreadedWorker[T] = componentFactory.spout(spout, scheduler)

  private val graphBuilderworker: List[ThreadedWorker[T]] =
    componentFactory.builder[T](graphBuilder, scheduler, schema)

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
