package com.raphtory.algorithms

import java.io._
import java.util

import org.apache.spark.sql.{Row, SparkSession}

object testAlgo extends App {
  val fl ="/home/tsunade/mlpa-test-output.parquet"
//  val fl ="/home/tsunade/Downloads/part-00080-tid-7987330100237236394-c9657f52-f64d-4541-bcb6-7626fbd90d27-734-1-c000.snappy.parquet"
  lazy val spark: SparkSession =   SparkSession.builder().master("local").getOrCreate()
  val br = spark.read.parquet(fl)
  var l = br.toDF("t","s","d","f")
  val t = l.reduce { (x, y) =>
    if (x.getAs[Long]("t") > y.getAs[Long]("t")) x else y
  }
  println(t)
}