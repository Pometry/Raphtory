package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.examples.coho.companiesStream.graphbuilders.CompaniesStreamPersonGraphBuilder
import com.raphtory.examples.coho.companiesStream.graphbuilders.CompaniesStreamRawGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.WebSocketSpout

/**
  * Runner to build a person to company network or raw network for a company.
  * Auth is set in the environment variables. Content Type and URL can be set in app conf of this directory.
  * The URL defines the stream you would like to pull from Companies House: https://developer-specs.company-information.service.gov.uk/streaming-api/reference
  * You will need to create an application with a key to get an authorization key to access this resource.
  */

object Runner extends App {

  val raphtoryConfig      = Raphtory.getDefaultConfig()
  private val auth        = raphtoryConfig.getString("raphtory.spout.coho.authorization")
  private val contentType = raphtoryConfig.getString("raphtory.spout.coho.contentType")
  private val url         = raphtoryConfig.getString("raphtory.spout.coho.url")
  val spout               = new WebSocketSpout(url, Some(auth), Some(contentType))
//  val builder = new CompaniesStreamRawGraphBuilder()
  val builder             = new CompaniesStreamPersonGraphBuilder()
  val source              = Source(spout, builder)
  val context             = Raphtory
  val graph               = context.newGraph()
  graph.ingest(source)

  val output = FileSink("/tmp/cohostream/people")
  Thread.sleep(10000)
  graph
    .climb("2022-12-31", "1 second")
    .past
    .execute(EdgeList())
    .writeTo(output)
    .waitForJob()
}
