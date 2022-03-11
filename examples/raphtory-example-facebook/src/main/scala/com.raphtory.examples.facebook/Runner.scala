package com.raphtory.examples.facebook

import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.config.PulsarController
import com.raphtory.deploy.Raphtory
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import com.raphtory.examples.facebook.graphbuilders.FacebookGraphBuilder
import com.raphtory.spouts.StaticGraphSpout
import com.typesafe.config
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.io.File
import scala.language.postfixOps
import sys.process._

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

  if (!new File("/tmp", "facebook.csv").exists()) {
    val status = {s"curl -o /tmp/facebook.csv https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv " !!}
    if (status != 0) {
      println(s"download of facebook failed with code $status")
      "rm /tmp/facebook.csv" !
    }
  }
  val conf: config.Config          = Raphtory.getDefaultConfig()

  val source: StaticGraphSpout  = StaticGraphSpout("/tmp/facebook.csv")
  val builder = new FacebookGraphBuilder()
  val graph = Raphtory.batchLoadGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  graph.pointQuery(EdgeList(), PulsarOutputFormat("EdgeList"), timestamp=88234)
  graph.rangeQuery(ConnectedComponents(), PulsarOutputFormat("ConnectedComponents"), 10000, 88234, 10000, List(500, 1000, 10000))

}
