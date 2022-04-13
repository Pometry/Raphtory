package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.deployment.Raphtory
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.io.File
import scala.language.postfixOps
import sys.process._

object PulsarOutputRunner extends App {
  // Set unlimited retention to keep topic
  val retentionTime = -1
  val retentionSize = -1

  val admin    = PulsarAdmin.builder
    .serviceHttpUrl("http://localhost:8080")
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build
  val policies = new RetentionPolicies(retentionTime, retentionSize)
  admin.namespaces.setRetention("public/default", policies)

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  if (!new File(path).exists())
    try s"curl -o $path $url" !!
    catch {
      case ex: Exception =>
        println(s"Failed to download 'lotr.csv' due to ${ex.getMessage}.")
        ex.printStackTrace()

        (s"rm $path" !)
        throw ex
    }

  // Create Graph
  val source  = FileSpout("/tmp/lotr.csv")
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)

  // Run algorihtms
  graph.pointQuery(EdgeList(), PulsarOutputFormat("EdgeList"), timestamp = 30000)
  graph.rangeQuery(
          PageRank(),
          PulsarOutputFormat("PageRank"),
          20000,
          30000,
          10000,
          List(500, 1000, 10000)
  )
}
