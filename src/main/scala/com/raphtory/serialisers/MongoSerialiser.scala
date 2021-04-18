package com.raphtory.serialisers

import java.net.InetAddress

import com.mongodb.DBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.util.JSON
import com.raphtory.core.analysis.api.AggregateSerialiser
import java.net.InetAddress
import scala.collection.JavaConverters._

object MongoSerialiser {
  new MongoSerialiser
}

class MongoSerialiser extends AggregateSerialiser{
  private val saveData  = System.getenv().getOrDefault("ANALYSIS_SAVE_OUTPUT", "false").trim.toBoolean
  private val mongoIP   = System.getenv().getOrDefault("ANALYSIS_MONGO_HOST", "localhost").trim
  private val mongoPort = System.getenv().getOrDefault("ANALYSIS_MONGO_PORT", "27017").trim
  private val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim
  private val mongo = MongoClient(MongoClientURI(s"mongodb://${InetAddress.getByName(mongoIP).getHostAddress}:$mongoPort"))

  override def serialiseView(results: Map[String, Any], timestamp: Long, jobID: String, viewTime: Long): Unit = {
    val data = (Map[String,Any]("time"->timestamp,"viewTime"->viewTime)++results).map{
      case (key,value) => valueToString(key,value)
    }.mkString("{",",","}")
    if (data.nonEmpty) mongo.getDB(dbname).getCollection(jobID).insert(JSON.parse(data).asInstanceOf[DBObject])
  }

  override def serialiseWindowedView(results: Map[String, Any], timestamp: Long, window: Long, jobID: String, viewTime: Long): Unit = {
    val data = (Map[String,Any]("time"->timestamp,"windowSize"->window,"viewTime"->viewTime)++results).map{
      case (key,value) => valueToString(key,value)
    }.mkString("{",",","}")
    if (data.nonEmpty) mongo.getDB(dbname).getCollection(jobID).insert(JSON.parse(data).asInstanceOf[DBObject])
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

}
