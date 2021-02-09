package com.raphtory.testCases.wordSemantic.spouts

import java.util

import com.raphtory.core.actors.Spout.Spout
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class CoMatParquetSpout(args: Array[String]) extends Spout[Row] {
  //args: [range, thr]
  lazy val spark: SparkSession =
    SparkSession.builder().master("local").getOrCreate()
  val containername = "samples"
  val storageacc    = "raphtorydatasets"
  val key           = "4OKgUnzvERASIYSwe5qKCC++foRxICHJw6XDkQvK0Bg0MGlhGeeyZfB+FC3zhuHNsDGGR/73VZTqIX64i/phkg=="

  private val directory          = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/sample-parquet").trim
  val Array(r1, r2)              = if (args.isEmpty) Array(2004L, 2008L) else args.head.trim.split("-").map(_.toLong)
  val thr: Double                = if (args.length < 1) 0.1 else args(1).trim.toDouble
  var merged: util.Iterator[Row] = spark.emptyDataFrame.toLocalIterator()

  override def setupDataSource(): Unit = {
    spark.conf.set("fs.azure.account.key." + storageacc + ".blob.core.windows.net", key)
    val source        = "wasbs://%s@%s.blob.core.windows.net/results/%s/sample-%s".format(containername, storageacc, directory, thr)
    var df: DataFrame = spark.createDataFrame(Seq.empty[(Long, String, String, Long)])
    for (y <- r1 to r2) {
      val mergedDF =
        spark.read.option("mergeSchema", "true").parquet("%s/D-%s-%s".format(source, thr, y))
      df = df.union(mergedDF)
    }
    merged = df.toLocalIterator()
  }

  override def generateData(): Option[Row] = //im: rewrite this to read pne parquet at a time
    if (merged.hasNext)
      Some(merged.next())
    else {
      dataSourceComplete()
      None
    }

  override def closeDataSource(): Unit = {}

}
