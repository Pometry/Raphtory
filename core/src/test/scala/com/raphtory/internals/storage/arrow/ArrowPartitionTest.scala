package com.raphtory.internals.storage.arrow

import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.Properties
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import munit.FunSuite

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}
import java.util.zip.GZIPInputStream

class ArrowPartitionTest extends FunSuite {
  test("how fast can we load from one single gzipped file on a single Arrow Partition") {

    val file   = "/pometry/wip/alphabay_sorted.csv.gz"
    val parOut = Paths.get("/pometry/wip/scala_arrow_data")
    val br     = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file), 65536)), 65536);

    val config    = ConfigBuilder.getDefaultConfig
    val par       = ArrowPartition(
            "testos",
            ArrowPartitionConfig(
                    config,
                    0,
                    ArrowSchema[VertexProp, EdgeProp],
                    parOut
            ),
            config
    )
    var i         = 1
    val emptyProp = Properties()
    val start = LocalDateTime.now()
    br.lines().forEach { line =>
      val fields      = line.split(",");
      val srcGlobalId = fields(3).toLong
      val dstGlobalId = fields(4).toLong
      val time        = fields(5).toLong
      val price       = fields(7).toLong

      par.addVertex(1, time, i, srcGlobalId, emptyProp, None)
      par.addVertex(1, time, i, dstGlobalId, emptyProp, None)
      par.addLocalEdge(1, time, i, srcGlobalId, dstGlobalId, Properties(MutableLong("weight", price)), None)
      i += 1

    }

    val end = LocalDateTime.now()

    println(s"Loading All of AlphaBay too ${Duration.between(start, end).toSeconds}s also known as ${Duration.between(start, end).toMinutes}min")

  }
}
