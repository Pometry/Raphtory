package com.raphtory.examples.twittercircles

import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.deploy.Raphtory
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.io.File
import scala.language.postfixOps
import sys.process._
import com.raphtory.examples.twittercircles.graphbuilders.TwitterCirclesGraphBuilder
import com.raphtory.spouts.StaticGraphSpout


object Runner extends App{
  //Set unlimited retention to keep topic
  val retentionTime = -1
  val retentionSize = -1
  val admin = PulsarAdmin.builder.serviceHttpUrl("http://localhost:8080")
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build
  val policies = new RetentionPolicies(retentionTime, retentionSize)
  admin.namespaces.setRetention("public/default", policies)

  // Create Graph
  if (!new File("/tmp", "snap-twitter.csv").exists()) {
    val status = {s"curl -o /tmp/snap-twitter.csv https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv " !}
    if (status != 0) {
      println(s"download of twitter failed with code $status")
      "rm /tmp/snap-twitter.csv" !
    }
  }
  val source  = StaticGraphSpout("/tmp/snap-twitter.csv")
  val builder = new TwitterCirclesGraphBuilder()
  val graph = Raphtory.batchLoadGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  graph.pointQuery(EdgeList(), PulsarOutputFormat("TwitterEdgeList"), 88234)
  graph.rangeQuery(ConnectedComponents(), PulsarOutputFormat("ConnectedComponents"), 10000, 88234, 10000, List(500, 1000, 10000))
}
