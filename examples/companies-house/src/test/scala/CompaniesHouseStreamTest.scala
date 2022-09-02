package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.examples.coho.companiesStream.graphbuilders.{CompaniesStreamPersonGraphBuilder, CompaniesStreamRawGraphBuilder}
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.WebSocketSpout

/**
 * Runner to build a person to company network or raw network for a company.
 * Auth is set in the environment variables. Content Type and URL can be set in app conf of this directory.
 * The URL defines the stream you would like to pull from Companies House: https://developer-specs.company-information.service.gov.uk/streaming-api/reference
 * You will need to create an application with a key to get an authorization key to access this resource.
 * To do this, you will need to create a Companies House Developer account: https://developer.company-information.service.gov.uk/
 */

object CompaniesHouseStreamTest extends App {

  val raphtoryConfig               = Raphtory.getDefaultConfig()
  private val auth = raphtoryConfig.getString("raphtory.spout.coho.authorization")
  private val contentType = raphtoryConfig.getString("raphtory.spout.coho.contentType")
  private val url = raphtoryConfig.getString("raphtory.spout.coho.url")
  val spout = new WebSocketSpout(url, Some(auth), Some(contentType))
  val rawBuilder = new CompaniesStreamRawGraphBuilder()
  val personBuilder = new CompaniesStreamPersonGraphBuilder()
  val source = Source(spout, rawBuilder)
  val context = Raphtory
  val graph = context.newGraph()
  val output = FileSink("/tmp/")
  graph.load(source)
  graph
    .climb("2022-12-31", "1 second")
    .past()
    .execute(EdgeList())
    .writeTo(output)
    .waitForJob()
}
