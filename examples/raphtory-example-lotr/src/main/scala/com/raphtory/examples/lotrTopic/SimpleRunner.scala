package com.raphtory.examples.lotrTopic

import com.raphtory.deployment.Raphtory
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils

object SimpleRunner extends App {
    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

    FileUtils.curlFile(path, url)

    val source  = FileSpout(path)
    val builder = new LOTRGraphBuilder()
    val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
}
