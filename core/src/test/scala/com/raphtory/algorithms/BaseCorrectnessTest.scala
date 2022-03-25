package com.raphtory.algorithms

import java.io.File
import java.nio.charset.StandardCharsets
import com.google.common.hash.Hashing
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.querymanager.JobDone
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.spout.Spout
import com.raphtory.components.spout.SpoutExecutor
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

abstract class BaseCorrectnessTest extends AnyFunSuite {

  private val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()
  implicit val schema: Schema[String]    = Schema.STRING
  val testDir                            = "/tmp/raphtoryTest" //TODO CHANGE TO USER PARAM
  val defaultOutputFormat: OutputFormat  = FileOutputFormat(testDir)

  def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder()

  def setSpout(resource: String): Spout[String] =
    ResourceSpout(resource)

  def correctResultsHash(resultsResource: String): String = {
    val results =
      scala.io.Source.fromResource(resultsResource).getLines().toList.sorted.flatten.toArray
    Hashing.sha256().hashString(new String(results), StandardCharsets.UTF_8).toString
  }

  def algorithmPointTest(
      algorithm: GraphAlgorithm,
      graphResource: String,
      lastTimestamp: Long,
      outputFormat: OutputFormat = defaultOutputFormat
  ): String = {
    val graph = Raphtory.streamGraph(setSpout(graphResource), setGraphBuilder())

    val conf                 = graph.getConfig()
    val startingTime         = System.currentTimeMillis()
    val queryProgressTracker = graph.pointQuery(algorithm, outputFormat, lastTimestamp)
    val jobId                = queryProgressTracker.getJobId()
    queryProgressTracker.waitForJob()
    graph.stop()

    val dirPath = new File(testDir + s"/$jobId")
    var results = Array.empty[Char]
    if (dirPath.isDirectory) {
      val dir = dirPath.listFiles.filter(_.isFile)
      results =
        (for (i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted.flatten
    }
    Hashing.sha256().hashString(new String(results), StandardCharsets.UTF_8).toString
  }

  def correctnessTest(
      algorithm: GraphAlgorithm,
      graphResource: String,
      resultsResource: String,
      lastTimestamp: Int
  ): Boolean =
    algorithmPointTest(algorithm, graphResource, lastTimestamp) == correctResultsHash(
            resultsResource
    )

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive()
}
