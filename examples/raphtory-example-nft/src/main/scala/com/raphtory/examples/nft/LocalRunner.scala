package com.raphtory.examples.nft

import com.raphtory.Raphtory
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder
import com.raphtory.formats.JsonFormat
import com.raphtory.sinks.{FileSink, PrintSink}
import com.raphtory.spouts.FileSpout

object LocalRunner extends App {

        val path   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_testOne.csv"

        val source  = FileSpout(path)
        val builder = new NFTGraphBuilder()
        val graph   = Raphtory.load(spout = source, graphBuilder = builder)
        val fileOutput  = FileSink("/tmp/nftoutput")
        val filepath = "/tmp/cyclesdata"
        val output   = FileSink(filepath, format = JsonFormat())

        // 1597947153 - 2020-08-20 19:12:33
        // 1619562578 - '2021-04-27 23:29:38'
        // 1595884980  - 2020-07-27 22:23:00
        val atTime = 1595884980

        graph
          .at(atTime)
          .past()
          .execute(CycleMania())
          .writeTo(output)

}
