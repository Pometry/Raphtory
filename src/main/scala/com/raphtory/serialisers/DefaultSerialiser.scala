package com.raphtory.serialisers

import com.raphtory.core.model.algorithm.AggregateSerialiser

import scala.reflect.io.Path

case class Nested(string:String)
class DefaultSerialiser extends AggregateSerialiser{
  private val output_file: String = System.getenv().getOrDefault("OUTPUT_PATH", "").trim

  override def serialiseView(results: Map[String, Any], timestamp: Long, jobID: String, viewTime: Long): Unit = {
    val data = (Map[String,Any]("time"->timestamp,"viewTime"->viewTime)++results).map{
      case (key,value) => valueToString(key,value)
    }.mkString("{",",","}")
    if (results.nonEmpty)  writeOut(data, output_file)
  }

  override def serialiseWindowedView(results: Map[String, Any], timestamp: Long, window: Long, jobID: String, viewTime: Long): Unit = {
    val data = (Map[String,Any]("time"->timestamp,"windowsize"->window,"viewTime"->viewTime)++results).map{
      case (key,value) => valueToString(key,value)
    }.mkString("{",",","}")
    if (results.nonEmpty)  writeOut(data, output_file)
  }

  private def individualValue(value:Any):String = value match {
    case v:String => s""""$v""""
    case v:Long   => s"""${v.toString}"""
    case v:Int    => s"""${v.toString}"""
    case v:Double => s"""${v.toString}"""
    case v:Float  => s"""${v.toString}"""
    case v:Nested => v.string
    case v:Array[String] => v.map(individualValue).mkString("[", ",", "]") //TODO compress to one array type, cases being a pain in the ass
    case v:Array[Int] => v.map(individualValue).mkString("[", ",", "]")
    case v:Array[Double] => v.map(individualValue).mkString("[", ",", "]")
    case v:Array[Float] => v.map(individualValue).mkString("[", ",", "]")
    case v:Array[Nested] => v.map(individualValue).mkString("[", ",", "]")
    case v: List[String] => v.map(individualValue).mkString("[", ",", "]") //TODO compress to one array type, cases being a pain in the ass
    case v: List[Int] => v.map(individualValue).mkString("[", ",", "]")
    case v: List[Double] => v.map(individualValue).mkString("[", ",", "]")
    case v: List[Float] => v.map(individualValue).mkString("[", ",", "]")
    case v: Map[String, Any] => v.map { case (key, value) => valueToString(key, value) }.mkString("{", ",", "}")
  }
  
  private def valueToString(key: String, value: Any):String = s""""$key":${individualValue(value)}"""
  def writeOut(data:String, outpath: String): Unit ={
    outpath match {
      case "" => println(data)
      case _  => Path(outpath).createFile().appendAll(data + "\n")
    }
  }
}
