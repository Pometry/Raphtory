package com.raphtory.ethereumtest

import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.deploy.Raphtory
import scala.language.postfixOps
import sys.process._
import java.io.File

object RaphtoryEthTest {

  def main(args: Array[String]): Unit = {

    val fileName = "/tmp/data"

  //    if (!new File(fileName).exists())
  //      s"curl -o '${fileName}' https://raw.githubusercontent.com/Raphtory/Data/main/transactions_03300000_03399999_small.csv.gz " !

    val spout: Spout[String] = FileSpout(fileName)
    val gb                   = new EthereumGraphBuilder()
    val graph                = Raphtory.createGraph(spout, gb)
  }
}
