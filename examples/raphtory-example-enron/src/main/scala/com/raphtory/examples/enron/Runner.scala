package com.raphtory.examples.enron

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.io.File
import scala.language.postfixOps
import sys.process._
import com.raphtory.examples.enron.graphbuilders.EnronGraphBuilder
import com.raphtory.spouts.ResourceSpout

object Runner extends App {
  //Set unlimited retention to keep topic
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
  val source  = ResourceSpout("email_test.csv")
  val builder = new EnronGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)

//  graph.rangeQuery(GraphState(), output, start = 1, end = 32674, increment = 10000, windows = List(500, 1000, 10000))
  graph.pointQuery(EdgeList(), PulsarOutputFormat("EdgeList"), timestamp = 989858340000L)
  graph.rangeQuery(
          ConnectedComponents(),
          PulsarOutputFormat("ConnectedComponents"),
          start = 963557940000L,
          end = 989858340000L,
          increment = 1000000000,
          windows = List()
  )
}
