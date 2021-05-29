package com.raphtory.serialisers

import com.raphtory.core.analysis.api.AggregateSerialiser

import scala.reflect.io.Path


class DefaultSerialiser extends AggregateSerialiser{
  private val saveData  = System.getenv().getOrDefault("ANALYSIS_SAVE_OUTPUT", "false").trim.toBoolean
  private val mongoIP   = System.getenv().getOrDefault("ANALYSIS_MONGO_HOST", "localhost").trim
  private val mongoPort = System.getenv().getOrDefault("ANALYSIS_MONGO_PORT", "27017").trim
  private val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim
//  private val mongo = MongoClient(MongoClientURI(s"mongodb://${InetAddress.getByName(mongoIP).getHostAddress}:$mongoPort"))
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
    case v:Array[String] => v.map(individualValue).mkString("[", ",", "]") //TODO compress to one array type, cases being a pain in the ass
    case v:Array[Int] => v.map(individualValue).mkString("[", ",", "]")
    case v:Array[Double] => v.map(individualValue).mkString("[", ",", "]")
    case v:Array[Float] => v.map(individualValue).mkString("[", ",", "]")

  }
  
  private def valueToString(key: String, value: Any):String = s""""$key":${individualValue(value)}"""
  def writeOut(data:String, outpath: String): Unit ={
    outpath match {
      case "" => println(data)
      //        case "mongo" => mongo.getDB(dbname).getCollection(jobID).insert(JSON.parse(data).asInstanceOf[DBObject])
      case _  => Path(outpath).createFile().appendAll(data + "\n")
    }
  }
}
