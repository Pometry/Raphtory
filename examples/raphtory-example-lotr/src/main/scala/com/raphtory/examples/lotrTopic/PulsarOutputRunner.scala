package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.deploy.Raphtory
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.ResourceSpout
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

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

  // Create Graph
  val source  = ResourceSpout("lotr.csv")
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
