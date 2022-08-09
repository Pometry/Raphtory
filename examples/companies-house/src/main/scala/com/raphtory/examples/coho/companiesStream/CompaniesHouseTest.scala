package com.raphtory.examples.coho.companiesStream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.spouts.FileSpout
import com.raphtory.sinks.FileSink
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.examples.coho.companiesStream.graphbuilders.{CompanyToPscBulkGraphBuilder, CompanyToPscGraphBuilder}

object CompaniesHouseTest {

  def main(args: Array[String]) {

    val source = FileSpout("/Users/rachelchan/Downloads/persons-with-significant-control-snapshot-2022-08-09.txt", regexPattern = "^.*\\.([jJ][sS][oO][nN]??)$")
    val builder = new CompanyToPscBulkGraphBuilder()
    val output = FileSink("/Users/rachelchan/psc/")
    val graph = Raphtory.stream[String](source, builder)

    graph
      .range("2000-01-01", "2022-08-04", "1 day")
      .window("1 day", Alignment.END)
      .execute(EdgeList())
      .writeTo(output)
  }

}
