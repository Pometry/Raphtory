package com.raphtory.internals.components.spout

import cats.effect.unsafe.implicits.global
import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.graphbuilder.BuilderExecutor
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

//class IngestionManager(
//    deploymentID: String,
//    scheduler: Scheduler,
//    conf: Config,
//    topics: TopicRepository
//) extends Component[EstablishGraph[_]](conf) {
//
//  case class Graph(spoutCancel: IO[Unit], builderCancel: IO[Unit]) {
//
//    def stop(): Unit = {
//      spoutCancel.unsafeRunSync()
//      builderCancel.unsafeRunSync()
//    }
//  }
//
//  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
//
//  private val graphDeployments = mutable.Map[String, Graph]()
//
//  override def handleMessage(msg: EstablishGraph[_]): Unit =
//    msg match {
//      case EstablishGraph(graphID, spout, builder) =>
//        val spoutResource      = SpoutExecutor[IO, _](spout, conf, topics)
//        val builderResource    = BuilderExecutor[IO, _](0, deploymentID, builder, conf, topics)
//        val (_, spoutCancel)   = spoutResource.allocated.unsafeRunSync()
//        val (_, builderCancel) = builderResource.allocated.unsafeRunSync()
//        graphDeployments.put(graphID, Graph(spoutCancel, builderCancel))
//    }
//
//  override private[raphtory] def run(): Unit = logger.debug(s"Starting IngestionManager")
//
//  override private[raphtory] def stop(): Unit =
//    graphDeployments.foreach { case (graphID, _) => graphDeployments.remove(graphID).foreach(_.stop()) }
//}
//
//object IngestionManager {
//
//  def apply[IO[_]: Async: Spawn](
//      deploymentID: String,
//      scheduler: Scheduler,
//      conf: Config,
//      topics: TopicRepository
//  ): Resource[IO, IngestionManager] =
//    Component.makeAndStart(
//            topics,
//            s"ingestion-manager",
//            List(topics.ingestSetup),
//            new IngestionManager(deploymentID, scheduler, conf, topics)
//    )
//}
