package com.raphtory

import cats.effect._
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import cats.effect.unsafe.implicits.global

/**  `Raphtory` object for creating Raphtory Components
  *
  * @example
  * {{{
  * import com.raphtory.Raphtory
  * import com.raphtory.spouts.FileSpout
  * import com.raphtory.api.analysis.graphstate.GraphState
  * import com.raphtory.sinks.FileSink
  *
  * val builder = new YourGraphBuilder()
  * val graph = Raphtory.stream(FileSpout("/path/to/your/file"), builder)
  * graph
  *   .range(1, 32674, 10000)
  *   .windows(List(500, 1000, 10000))
  *   .execute(GraphState())
  *   .writeTo(FileSink("/test_dir"))
  *
  * graph.deployment.stop()
  * }}}
  *
  * @see [[GraphBuilder GraphBuilder]]
  *      [[api.input.Spout Spout]]
  *      [[api.analysis.graphview.DeployedTemporalGraph DeployedTemporalGraph]]
  *      [[api.analysis.graphview.TemporalGraph TemporalGraph]]
  */
object Raphtory {

  private lazy val deployInterface = defaultConf.getString("raphtory.deploy.address")
  private lazy val deployPort      = defaultConf.getInt("raphtory.deploy.port")

  private lazy val serviceLocal = {
    val (serviceLocal, shutdown) = RaphtoryServiceBuilder.standalone[IO](defaultConf).allocated.unsafeRunSync()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        shutdown.unsafeRunSync()
    })

    serviceLocal
  }

  def local(): RaphtoryContext =
    new RaphtoryContext(Resource.pure(serviceLocal), defaultConf, true)

  def remote(interface: String = deployInterface, port: Int = deployPort): RaphtoryContext = {
    val config =
      ConfigBuilder()
        .addConfig("raphtory.deploy.address", interface)
        .addConfig("raphtory.deploy.port", port)
        .build()
        .getConfig

    val service = RaphtoryServiceBuilder.client[IO](config)
    new RaphtoryContext(service, config, false)
  }
}
