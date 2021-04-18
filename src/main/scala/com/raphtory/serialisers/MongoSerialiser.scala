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
    val data = (results + (("timestamp",timestamp)) + (("viewTime",viewTime))) .map{
      case (key,value) => valueToString(key,value)
    }.mkString("{",",","}")
    val json = JSON.parse(data).asInstanceOf[DBObject]
    if (data.nonEmpty) mongo.getDB(dbname).getCollection(jobID).insert(json)
  }

  override def serialiseWindowedView(results: Map[String, Any], timestamp: Long, window: Long, jobID: String, viewTime: Long): Unit = ???

  private def valueToString(key: String, value: Any):String = {
    val realValue= value match {
      case v:Array[Any] => v.map(individualValue).mkString("[",",","]")
      case v:Any => individualValue(v)
    }
    s""""$key":$realValue"""
  }

  private def individualValue(value:Any) = value match {
    case v:String => s""""$v""""
    case v:Long   => s"""$v"""
    case v:Int    => s"""$v"""
    case v:Double => s"""$v"""
    case v:Float  => s"""$v"""
  }


}
