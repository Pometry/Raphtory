package com.raphtory.ethereumtest

import com.raphtory.Raphtory
import com.raphtory.api.input.Spout

import scala.language.postfixOps
import sys.process._
import java.io.File
import com.raphtory.spouts.FileSpout

object RaphtoryEthTest {

  def main(args: Array[String]): Unit = {

    val fileName = "/tmp/data"

    //    if (!new File(fileName).exists())
    //      s"curl -o '${fileName}' https://raw.githubusercontent.com/Raphtory/Data/main/transactions_03300000_03399999_small.csv.gz " !

    val spout: Spout[String] = FileSpout(fileName)
    val gb                   = new EthereumGraphBuilder()
    val graph                = Raphtory.stream(spout, gb)
  }
}
