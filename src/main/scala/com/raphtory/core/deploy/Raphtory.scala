package com.raphtory.core.deploy

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.ConfigHandler
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.raphtory.core.client.RaphtoryClient
import com.raphtory.core.client.RaphtoryGraph
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.StaticGraphSpout
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object Raphtory {

  private val scheduler = new MonixScheduler().scheduler

  def streamGraph[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): RaphtoryGraph[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    new RaphtoryGraph[T](
            false,
            spout,
            graphBuilder,
            conf,
            componentFactory,
            scheduler,
            pulsarController
    )
  }

  def batchLoadGraph[T: ClassTag: TypeTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): RaphtoryGraph[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    new RaphtoryGraph[T](
            true,
            spout,
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
    componentFactory.spout(spout, false, scheduler)
  }

  def createGraphBuilder[T: ClassTag](
      builder: GraphBuilder[T]
  ): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.builder(builder, false, scheduler)
  }

  def createPartitionManager[T: ClassTag](
      batchLoading: Boolean = false,
      spout: Option[Spout[T]] = None,
      graphBuilder: Option[GraphBuilder[T]] = None
  ): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.partition(scheduler, batchLoading, spout, graphBuilder)
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

<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
      case IdentitySpout() => new IdentitySpoutExecutor[T](conf, pulsarController)
      case ResourceSpout(resource) =>
        new ResourceSpoutExecutor(resource, conf, pulsarController).asInstanceOf[SpoutExecutor[T]]
      case StaticGraphSpout(fileDataPath) =>
        new StaticGraphSpoutExecutor(fileDataPath, conf, pulsarController).asInstanceOf[SpoutExecutor[T]]
=======
      case IdentitySpout() => new IdentitySpoutExecutor[T](conf, pulsarController, scheduler)
      case ResourceSpout(resource) =>
        new ResourceSpoutExecutor(resource, conf, pulsarController, scheduler).asInstanceOf[SpoutExecutor[T]]
      case StaticGraphSpout(fileDataPath) =>
        new StaticGraphSpoutExecutor(fileDataPath, conf, pulsarController, scheduler).asInstanceOf[SpoutExecutor[T]]
>>>>>>> ca725f74 (Reenabled file polling via scheduler)
=======
      case IdentitySpout()                => new IdentitySpoutExecutor[T](conf, pulsarController, scheduler)
      case ResourceSpout(resource)        =>
        new ResourceSpoutExecutor(resource, conf, pulsarController, scheduler)
          .asInstanceOf[SpoutExecutor[T]]
      case StaticGraphSpout(fileDataPath) =>
        new StaticGraphSpoutExecutor(fileDataPath, conf, pulsarController, scheduler)
          .asInstanceOf[SpoutExecutor[T]]
>>>>>>> bda178d9 (Updated Scalafmt to 2.6.3)

    }

=======
>>>>>>> c7bd1089 (Created new spout API and added batchloading to the partitions (#271))
}
