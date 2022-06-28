package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.examples.coho.companiesStream.graphbuilders.CompaniesStreamRawGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.WebSocketSpout

object Runner extends App {

  val raphtoryConfig               = Raphtory.getDefaultConfig()
  private val auth = raphtoryConfig.getString("raphtory.spout.coho.authorization")
  private val contentType = raphtoryConfig.getString("raphtory.spout.coho.contentType")
  private val url = raphtoryConfig.getString("raphtory.spout.coho.url")
  val source = new WebSocketSpout(url, Some(auth), Some(contentType))
  val builder = new CompaniesStreamRawGraphBuilder()
  val graph = Raphtory.stream(spout = source, graphBuilder = builder)
  val output = FileSink("/tmp/cohostream")
  graph
    .walk("10 seconds")
    .window("10 seconds")
    .execute(EdgeList())
    .writeTo(output)
}
