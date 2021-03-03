//package com.raphtory.algorithms
//
//import java.io._
//import java.util
//
//import org.apache.spark.sql.{Row, SparkSession}
//
//object testAlgo extends App {
//    val fl ="/home/tsunade/mlpa-test-output.parquet"
//  //  val fl ="/home/tsunade/Downloads/part-00080-tid-7987330100237236394-c9657f52-f64d-4541-bcb6-7626fbd90d27-734-1-c000.snappy.parquet"
//    lazy val spark: SparkSession =   SparkSession.builder().master("local").getOrCreate()
//    case class Data(word:String, year:Long, comm:Long)
//    val br = spark.read.parquet(fl)
//    val a = br.rdd.zipWithIndex.flatMap(x=> processComm(x._1,x._2))//.zipWithIndex(_.split("_")))
//  spark.createDataFrame(a).write.mode("overwrite").parquet("/home/tsunade/test.parquet")
//
////  println(a.length)
//
//
//    def processComm(c: Row, id: Long): Seq[(String,Long,Long)] ={
//      val a =  c.getAs[Seq[String]]("comm").map(_.split("_"))
//      a.map(x=> (x.head, x.last.toLong, id))
//    }
//
////  var l = br.toDF("t","s","d","f")
////  val t = l.reduce { (x, y) =>
////    if (x.getAs[Long]("t") > y.getAs[Long]("t")) x else y
////  }
////  println(t)
//}