package com.raphtory.examples.nft

import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils
import com.raphtory.examples.nft.graphbuilder.NFTGraphBuilder

object LocalRunner extends App {

        val path   = "/Users/haaroony/OneDrive - Pometry Ltd/nft_andrea/Data_API_100k.csv"

        val source  = FileSpout(path)
        val builder = new NFTGraphBuilder()
        val graph   = Raphtory.batchLoad(spout = source, graphBuilder = builder)
}
