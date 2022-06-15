package com.raphtory.examples.nft

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder

object LocalRunner extends App {

        val path   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_100k.csv"

        val source  = FileSpout[String](path)
        val builder = new NFTGraphBuilder()
        val graph   = Raphtory.batchLoad(spout = source, graphBuilder = builder)

        graph
          .at(1577836785)
          .past()
          .execute(CycleMania())
          .writeTo(FileOutputFormat("/tmp/run1"))

}
