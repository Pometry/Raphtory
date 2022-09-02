package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.api.input.Source
import com.raphtory.spouts.FileSpout
import com.raphtory.sinks.FileSink
import com.raphtory.examples.coho.companiesStream.algorithms.CompanyDirectorGraph
import com.raphtory.examples.coho.companiesStream.graphbuilders.{CompanyToPscBulkGraphBuilder, CompanyToPscGraphBuilder}
import com.raphtory.formats.JsonFormat

/**
 * Runner to build Company to PSC Graph, in addition to PSC to PSC graph,
 * with edges weighted by share ownership and labelled with date PSC notified on.
 */
object CompaniesHouseTest {

  def main(args: Array[String]) {

    val spout = FileSpout("/Users/rachelchan/Documents/nhsContractsPSCData", regexPattern = "^.*\\.([jJ][sS][oO][nN]??)$", recurse = true)
    val builder = new CompanyToPscGraphBuilder()
    val output = FileSink("/Users/rachelchan/Downloads/psc",JsonFormat())
    val source = Source(spout, builder)
    val graph = Raphtory.newGraph()

    graph.load(source)
    graph
      .at("2022-08-04")
      .past()
      .execute(new CompanyDirectorGraph())
      .writeTo(output)
      .waitForJob()

    graph.close()
  }

}
