package com.raphtory.examples.lotrTopic

import com.raphtory.api.input.Spout
import com.raphtory.RaphtoryService
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils
import com.typesafe.config.ConfigFactory

object LOTRService extends RaphtoryService[String] {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)


  override def defineSpout(): Spout[String] = FileSpout(path)

  override def defineBuilder: LOTRGraphBuilder = new LOTRGraphBuilder()

}