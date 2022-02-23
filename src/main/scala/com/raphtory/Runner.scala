package com.raphtory

import com.raphtory.algorithms.generic.centrality.AverageNeighbourDegree
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.graphbuilder.ImmutableProperty
import com.raphtory.core.components.graphbuilder.Properties
import com.raphtory.core.components.graphbuilder.Type
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.StaticGraphSpout
import com.raphtory.core.config.PulsarController
import com.raphtory.core.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import org.apache.pulsar.client.api.Schema

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

object Runner extends App {

  val spout        = setSpout()
  val graphBuilder = setGraphBuilder()
  val graph        = Raphtory.createTypedGraph[String](spout, graphBuilder, Schema.STRING)

  Raphtory.createClient("deployment123", Map(("raphtory.pulsar.endpoint", "localhost:1234")))

  val conf             = graph.getConfig()
  val pulsarController = new PulsarController(conf)

  val algorithm           = AverageNeighbourDegree()
  val start               = 1
  val end                 = 1400000
  val increment           = 100
  val windows: List[Long] = List(500, 1000, 10000)

  val outputFormat: FileOutputFormat = FileOutputFormat("/tmp/raphtoryTest")

  //  val queryProgressTracker = graph.rangeQuery(algorithm, outputFormat, start, end, increment, windows)
  //  val jobId                = queryProgressTracker.getJobId()
  //
  //  queryProgressTracker.waitForJob()

  def setSpout(): Spout[String] = StaticGraphSpout(s"/tmp/twitter.csv")

  def setGraphBuilder(): GraphBuilder[String] = new TwitterGraphBuilder()

  def setup(): Unit = {
    val data = new File("/tmp", "twitter.csv")

    if (!data.exists()) {
      val download = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"

      val status = s"curl -o /tmp/twitter.csv $download" !

      if (status != 0) {
        println("I fucked up.")
        "rm /tmp/twitter.csv" !
      }
    }
  }
}

class TwitterGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    val fileLine   = tuple.split(" ").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
  }
}
