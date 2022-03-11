package com.raphtory.core.deploy

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.executor.FileSpoutExecutor
import com.raphtory.core.components.spout.executor.IdentitySpoutExecutor
import com.raphtory.core.components.spout.executor.ResourceSpoutExecutor
import com.raphtory.core.components.spout.executor.StaticGraphSpoutExecutor
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.components.spout.instance.IdentitySpout
import com.raphtory.core.components.spout.instance.ResourceSpout
import com.raphtory.core.components.spout.instance.StaticGraphSpout
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.ConfigHandler
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.raphtory.core.client.RaphtoryClient
import com.raphtory.core.client.RaphtoryGraph
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * {s}`Raphtory`
  *  : `Raphtory` object for creating Raphtory Components
  *
  * ## Methods
  *
  *   {s}`createGraph(spout: Spout[T] = new IdentitySpout[T](), graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): RaphtoryGraph[T]`
  *    : Create Graph using spout, graph-builder and custom config
  *
  *   {s}`createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`
  *    : Create Client
  *
  *   {s}`createSpout(spout: Spout[T])`
  *    : Create Spout
  *
  *   {s}`createGraphBuilder(builder: GraphBuilder[T])`
  *    : Create Graph Builder
  *
  *   {s}`createPartitionManager()`
  *    : Creates Partition Manager
  *
  *   {s}`createQueryManager()`
  *    : Creates Query Manager
  *
  *   {s}`getDefaultConfig(customConfig: Map[String, Any] = Map()): Config`
  *    : Returns default config
  *
  *   {s}`confBuilder(customConfig: Map[String, Any] = Map()): Config`
  *    : Returns Config
  *
  *   {s}`createSpoutExecutor(spout: Spout[T], conf: Config, pulsarController: PulsarController): SpoutExecutor[T]]`
  *    : Create spout executor
  *
  *
  * Example Usage:
  *
  * ```{code-block} scala
  *
  * import com.raphtory.core.deploy.Raphtory
  * import com.raphtory.lotrtest.LOTRGraphBuilder
  * import com.raphtory.core.components.spout.instance.ResourceSpout
  *
  * val graph = Raphtory.createGraph(ResourceSpout("resource"), LOTRGraphBuilder())
  *
  * ```
  *
  */
object Raphtory {

  private val scheduler = new MonixScheduler().scheduler

  def createGraph[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): RaphtoryGraph[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val spoutExecutor    = createSpoutExecutor[T](spout, conf, pulsarController)
    new RaphtoryGraph[T](
            spoutExecutor,
            graphBuilder,
            conf,
            componentFactory,
            scheduler,
            pulsarController
    )
  }

  def createClient(
      deploymentID: String = "",
      customConfig: Map[String, Any] = Map()
  ): RaphtoryClient = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    new RaphtoryClient(deploymentID, conf, componentFactory, scheduler, pulsarController)
  }

  def createSpout[T](spout: Spout[T]): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val spoutExecutor    = createSpoutExecutor[T](spout, conf, pulsarController)
    componentFactory.spout(spoutExecutor, scheduler)
  }

  def createGraphBuilder[T: ClassTag](
      builder: GraphBuilder[T]
  ): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.builder(builder, scheduler)
  }

  def createPartitionManager(): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.partition(scheduler)
  }

  def createQueryManager(): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.query(scheduler)
  }

  def getDefaultConfig(customConfig: Map[String, Any] = Map()): Config =
    confBuilder(customConfig)

  private def confBuilder(customConfig: Map[String, Any] = Map()): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig
  }

  private def createSpoutExecutor[T](
      spout: Spout[T],
      conf: Config,
      pulsarController: PulsarController
  ): SpoutExecutor[T] =
    spout match {
      case spout: FileSpout[T]            =>
        new FileSpoutExecutor[T](
                spout.source,
                spout.schema,
                spout.lineConverter,
                conf,
                pulsarController,
                scheduler
        )
      case IdentitySpout()                => new IdentitySpoutExecutor[T](conf, pulsarController, scheduler)
      case ResourceSpout(resource)        =>
        new ResourceSpoutExecutor(resource, conf, pulsarController, scheduler)
          .asInstanceOf[SpoutExecutor[T]]
      case StaticGraphSpout(fileDataPath) =>
        new StaticGraphSpoutExecutor(fileDataPath, conf, pulsarController, scheduler)
          .asInstanceOf[SpoutExecutor[T]]

    }

}
