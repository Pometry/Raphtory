package com.raphtory.algorithms

import java.io.File
import java.nio.charset.StandardCharsets
import com.google.common.hash.Hashing
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.querymanager.JobDone
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.components.spout.instance.ResourceSpout
import com.raphtory.core.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.CompletableFuture
import scala.util.Random

class BaseCorrectnessTest extends AnyFunSuite {

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
    val graph = Raphtory.createGraph(setSpout(graphResource), setGraphBuilder())

    val conf                 = graph.getConfig()
    val startingTime         = System.currentTimeMillis()
    val queryProgressTracker = graph.pointQuery(algorithm, outputFormat, lastTimestamp)
    val jobId                = queryProgressTracker.getJobId()
    queryProgressTracker.waitForJob()
    graph.stop()

    val dir     = new File(testDir + s"/$jobId").listFiles.filter(_.isFile)
    val results =
      (for (i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted.flatten
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

  def receiveMessage(consumer: Consumer[Array[Byte]]): CompletableFuture[Message[Array[Byte]]]= {
    consumer.receiveAsync()
  }

}
