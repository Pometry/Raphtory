package com.raphtory.examples.nft

import com.raphtory.Raphtory
import com.raphtory.api.input.Source
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder
import com.raphtory.formats.JsonFormat
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

object LocalRunner extends App {

  val data = "/tmp/Data_API_clean_nfts_ETH_only.csv"

  val spout   = FileSpout(data)
  val builder = new NFTGraphBuilder()
  val source  = Source(spout, builder)
  val graph   = Raphtory.localContext().newGraph()
  graph.ingest(source)

  val resultsPath = "/tmp/raphtory_nft"
  val output      = FileSink(resultsPath, format = JsonFormat())

  val atTime = 1619564391

  graph
    .at(atTime)
    .past()
    .execute(CycleMania())
    .writeTo(output)
    .waitForJob()

  graph.close()
}
