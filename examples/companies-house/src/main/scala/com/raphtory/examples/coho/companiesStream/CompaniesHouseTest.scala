package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.spouts.FileSpout
import com.raphtory.sinks.FileSink
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.examples.coho.companiesStream.graphbuilders.CompanyToPscGraphBuilder

object CompaniesHouseTest {

  def main(args: Array[String]) {

    val source = FileSpout("", regexPattern = "^.*\\.([jJ][sS][oO][nN]??)$", recurse = true)
    val builder = new CompanyToPscGraphBuilder()
    val output = FileSink("")
    val graph = Raphtory.load[String](source, builder)

    graph
      .range("1900-01-01", "2022-08-04", "1 day")
      .window("1 day", Alignment.END)
      .execute(EdgeList())
      .writeTo(output)
  }

}
