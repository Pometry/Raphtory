package com.raphtory.core.deploy

import com.raphtory.core.algorithm.TemporalGraphBuilder
import com.raphtory.core.algorithm.TemporalGraph
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
import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.client.RaphtoryClient
import com.raphtory.core.client.GraphDeployment
import com.typesafe.config.Config

import scala.reflect.ClassTag

object Raphtory {

  private val scheduler = new MonixScheduler().scheduler

  def createGraph[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): GraphDeployment[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val spoutExecutor    = createSpoutExecutor[T](spout, conf, pulsarController)
    val queryBuilder     = new QueryBuilder(componentFactory, scheduler, pulsarController)
    new GraphDeployment[T](
            spoutExecutor,
            graphBuilder,
            queryBuilder,
            conf,
            componentFactory,
            scheduler
    )
  }

  def deployGraph[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): TemporalGraph = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val spoutExecutor    = createSpoutExecutor[T](spout, conf, pulsarController)
    val queryBuilder     = new QueryBuilder(componentFactory, scheduler, pulsarController)
    new GraphDeployment[T](
            spoutExecutor,
            graphBuilder,
            queryBuilder,
            conf,
            componentFactory,
            scheduler
    )

    new TemporalGraphBuilder(queryBuilder, conf)
  }

  def getGraph(customConfig: Map[String, Any] = Map()): TemporalGraph = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val queryBuilder     = new QueryBuilder(componentFactory, scheduler, pulsarController)
    new TemporalGraphBuilder(queryBuilder, conf)
  }

  def createClient(
      deploymentID: String = "",
      customConfig: Map[String, Any] = Map()
  ): RaphtoryClient = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val queryBuilder     = new QueryBuilder(componentFactory, scheduler, pulsarController)
    new RaphtoryClient(queryBuilder, conf)
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
