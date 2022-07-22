package com.raphtory.examples.nft

import com.raphtory.Raphtory
import com.raphtory.examples.nft.analysis.{CycleMania, CycleManiaONSquared}
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder
import com.raphtory.formats.{CsvFormat, JsonFormat}
import com.raphtory.sinks.{FileSink, PrintSink}
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object LocalRunner extends App {

      val data = "/tmp/Data_API_clean_nfts_ETH_only.csv"

      val source  = FileSpout(data)
      val builder = new NFTGraphBuilder()

      val graph   = Raphtory.load(spout = source, graphBuilder = builder)

      val resultsPath = "/tmp/raphtory_nft"
      val output   = FileSink(resultsPath, format = JsonFormat())

      val atTime = 1619564391

      graph
        .at(atTime)
        .past()
        .execute(CycleMania())
        .writeTo(output)
        .waitForJob()

      graph.close()
}