package com.raphtory.internals.management

import com.raphtory.api.input.Source
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.DynamicLoader
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.typesafe.config.Config

import scala.util.Random

private[raphtory] class QuerySender(
    private val scheduler: Scheduler,
    private val topics: TopicRepository,
    private val config: Config,
    private val clientID: String
) {

  private val graphID          = config.getString("raphtory.graph.id")
  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer
  private lazy val writers     = topics.graphUpdates(graphID).endPoint(-1)
  private lazy val submissions = topics.submissions.endPoint

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {
    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + Random.nextLong().abs
    val outputQuery = query.copy(name = jobID)

    topics.submissions.endPoint sendAsync outputQuery

    val tracker = QueryProgressTracker.unsafeApply(jobID, config, topics)
    scheduler.execute(tracker)
    tracker
  }

  def destroyGraph(force: Boolean): Unit =
    topics.graphSetup.endPoint sendAsync DestroyGraph(graphID, clientID, force)

  def disconnect(): Unit =
    topics.graphSetup.endPoint sendAsync ClientDisconnected(graphID, clientID)

  def establishGraph(): Unit =
    topics.graphSetup.endPoint sendAsync EstablishGraph(graphID, clientID)

  def individualUpdate(update: GraphUpdate) =
    writers((update.srcId % totalPartitions).toInt) sendAsync update

  def submitGraph(sources: Seq[Source], id: String): Unit = {

    val clazzes = sources
      .map { source =>
        source.getBuilder.getClass
      }
      .toSet
      .asInstanceOf[Set[Class[_]]]

    submissions sendAsync IngestData(DynamicLoader(clazzes), id, sources)
  }

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
