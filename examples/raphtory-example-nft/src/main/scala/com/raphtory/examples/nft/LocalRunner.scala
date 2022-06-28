package com.raphtory.examples.nft

import com.raphtory.Raphtory
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder
import com.raphtory.formats.{CsvFormat, JsonFormat}
import com.raphtory.sinks.{FileSink, PrintSink}
import com.raphtory.spouts.FileSpout

object LocalRunner extends App {

        val pathOne   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_testOne.csv"
        val path1m   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_1m.csv"

        val source  = FileSpout(path1m)
        val builder = new NFTGraphBuilder()

        val graph   = Raphtory.load(spout = source, graphBuilder = builder)

        val filepath = "/tmp/cyclesdata"
        val output   = FileSink(filepath, format = CsvFormat())

        // 1597947153 - 2020-08-20 19:12:33
        // 1619562578 - '2021-04-27 23:29:38'
        // 1595884980  - 2020-07-27 22:23:00 testOne
        val atTime = 1595884980

        graph
          .at(atTime)
          .past()
          .execute(CycleMania())
          .writeTo(output)
          .waitForJob()

        graph.close()
}
