package com.raphtory.examples.nft

import com.raphtory.Raphtory
import com.raphtory.examples.nft.analysis.{CycleMania, CycleManiaONSquared}
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder
import com.raphtory.formats.{CsvFormat, JsonFormat}
import com.raphtory.sinks.{FileSink, PrintSink}
import com.raphtory.spouts.FileSpout

object LocalRunner extends App {

        val pathOne   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_testOne.csv"
        val path1m   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_1m.csv"
        val path250k   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_250k.csv"
        val path500k   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_500k.csv"
        val pathBad =         "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/bad_nft.csv"
        val fullDataCleaned = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_cleaned_ETH_only.csv"
        val smallDataCleanedEthOnly = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_clean_nfts_ETH_only.csv"

        val t0 = System.currentTimeMillis()

        val source  = FileSpout(smallDataCleanedEthOnly)
        val builder = new NFTGraphBuilder()

        val graph   = Raphtory.load(spout = source, graphBuilder = builder)

        val filepath = "/tmp/cyclesdata"
        val output   = FileSink(filepath, format = CsvFormat())

        // 1597947153 - 2020-08-20 19:12:33
        // 1619562578 - '2021-04-27 23:29:38' test1m / 250k / 500k / ALL / eth cleaned / smallEthCleaned
        //   1613266168         - 2021-02-14 01:29:28
        // 1595884980  - 2020-07-27 22:23:00 testOne
        // 1540432748 2018-10-25 02:59:08 // badnft2
        //            - 2021-04-27 23:59:51
        val atTime = 1619562578

        graph
          .at(atTime)
          .past()
          .execute(CycleMania())
          .writeTo(output)
          .waitForJob()

        val t1 = System.currentTimeMillis()
        println("Elapsed time: " + (t1 - t0) + "ms")
        graph.close()
}

/*
500k
FAST Runs
34931ms
33012ms



Slow runs


 */