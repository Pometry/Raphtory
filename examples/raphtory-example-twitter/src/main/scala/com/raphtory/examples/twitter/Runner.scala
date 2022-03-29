package com.raphtory.examples.twitter

import com.raphtory.deploy.Raphtory
import com.raphtory.examples.twitter.graphbuilders.TwitterGraphBuilder
import com.raphtory.examples.twitter.analysis.MemberRank
import com.raphtory.examples.twitter.analysis.PageRank
import com.raphtory.examples.twitter.analysis.TemporalMemberRank
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.ResourceSpout
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

object Runner extends App {
  //Set unlimited retention to keep topic
  val retentionTime = -1
  val retentionSize = -1

  val admin         = PulsarAdmin.builder
    .serviceHttpUrl("http://localhost:8080")
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build
  val policies      = new RetentionPolicies(retentionTime, retentionSize)
  admin.namespaces.setRetention("public/default", policies)

  // Create Graph
  val source  = ResourceSpout("higgs-retweet-activity.csv")
  val builder = new TwitterGraphBuilder()
  val graph   = Raphtory.batchLoadGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  val output  = PulsarOutputFormat("Retweets")

  graph.rangeQuery(
          PageRank() -> MemberRank() -> TemporalMemberRank(),
          output,
          start = 1341101181,
          end = 1341705593,
          increment = 500000000,
          windows = List()
  )
}
