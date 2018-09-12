package com.raphtory.core.actors.partitionmanager

import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.{$addToSet, MongoDBList, MongoDBObject}
import com.mongodb.casbah.MongoConnection
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.{$addToSet, _}
import com.mongodb.casbah.MongoConnection
import net.liftweb.json._

import scala.collection.parallel.ParSet
object MongoFactory {

  private val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  implicit val formats = DefaultFormats // Brings in default date formats etc.

  private val DATABASE = "raphtory"
  val connection = MongoClient()

  val edges = connection(DATABASE)("edges")
  val vertices = connection(DATABASE)("vertices")
  vertices.remove(new MongoDBObject)
  edges.remove(new MongoDBObject)

  private var vertexOperator = vertices.initializeOrderedBulkOperation
  private var edgeOperator = edges.initializeOrderedBulkOperation


  def flushBatch() ={
    try {
      vertexOperator.execute()
    }
    catch {
      case e:Exception => e.printStackTrace()
    }

    try {
      edgeOperator.execute()

    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    vertexOperator = vertices.initializeOrderedBulkOperation
    edgeOperator = edges.initializeOrderedBulkOperation
  }

  def vertex2Mongo(entity:Vertex,cutoff:Long)={
    if(entity beenSaved()){
      updateVertex(entity,cutoff)
    }
    else{
      newVertex(entity,cutoff)
    }
  }

  private def newVertex(vertex:Vertex,cutOff:Long):Unit ={
    val history = vertex.compressAndReturnOldHistory(cutOff)
    if(history isEmpty)
      return
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> vertex.getId
    builder += "oldestPoint" -> vertex.oldestPoint.get
    builder += "history" -> convertHistory(history)
    builder += "properties" -> convertProperties(vertex.properties,cutOff)
    val set = vertex.getNewAssociatedEdges()
    if (set.nonEmpty) {
      builder += "associatedEdges" -> convertAssociatedEdges(set)
    }
    vertexOperator.insert(builder.result())
  }

   private def updateVertex(vertex: Vertex,cutOff:Long) ={
    val history = convertHistoryUpdate(vertex.compressAndReturnOldHistory(cutOff))
    val dbEntity = vertexOperator.find(MongoDBObject("_id" -> vertex.getId))
    dbEntity.updateOne($addToSet("history") $each(history:_*))

    for((key,property) <- vertex.properties){
      val entityHistory = convertHistoryUpdate(property.compressAndReturnOldHistory(cutOff))
      dbEntity.updateOne($addToSet(s"properties.$key") $each(entityHistory:_*)) //s"properties.$key"
    }
     val set = vertex.getNewAssociatedEdges()
     if (set.nonEmpty)
       dbEntity.updateOne($addToSet(s"associatedEdges") $each(convertAssociatedEdgesUpdate(set):_*))
  }

  private def convertAssociatedEdges(set:ParSet[Edge]):MongoDBList = {
    val builder = MongoDBList.newBuilder
    for(edge <- set){
      builder += edge.getId
    }
    println(builder.result())
    builder.result()
  }
  private def convertAssociatedEdgesUpdate(set:ParSet[Edge]):List[Long] = {
    val builder = mutable.ListBuffer[Long]()
    for(edge <- set){
      builder += edge.getId
    }
    builder.toList
  }


  def edge2Mongo(entity:Edge,cutoff:Long)={
    if(entity beenSaved()){
      updateEdge(entity,cutoff)
    }
    else{
      newEdge(entity,cutoff)
    }
  }


  private def newEdge(edge:Edge,cutOff:Long):Unit ={
    val history = edge.compressAndReturnOldHistory(cutOff)
    if(history isEmpty)
      return
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> edge.getId
    builder += "history" -> convertHistory(history)
    builder += "properties" -> convertProperties(edge.properties,cutOff)
    //convertProperties(builder,entity.properties,cutOff) // no outside properties list
    edgeOperator.insert(builder.result())
  }

   private def updateEdge(edge: Edge,cutOff:Long) ={
    val history = convertHistoryUpdate(edge.compressAndReturnOldHistory(cutOff))
    val dbEntity = edgeOperator.find(MongoDBObject("_id" -> edge.getId))
    dbEntity.updateOne($addToSet("history") $each(history:_*))

    for((key,property) <- edge.properties){
      val entityHistory = convertHistoryUpdate(property.compressAndReturnOldHistory(cutOff))
        dbEntity.updateOne($addToSet(s"properties.$key") $each(entityHistory:_*)) //s"properties.$key"
    }

  }


  private def convertHistory[b <: Any](history:mutable.TreeMap[Long,b]):MongoDBList ={
    val builder = MongoDBList.newBuilder
    for((k,v) <-history)
      if(v.isInstanceOf[String])
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[String])
      else
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[Boolean])
    builder.result
  }
  //these are different as the update requires a scala list which can be 'eached'
  private def convertHistoryUpdate[b <: Any](history:mutable.TreeMap[Long,b]):List[MongoDBObject] ={
    val builder = mutable.ListBuffer[MongoDBObject]()
    for((k,v) <-history)
      if(v.isInstanceOf[String])
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[String])
      else
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[Boolean])

    builder.toList
  }

  private def convertProperties(properties: ParTrieMap[String,Property],cutOff:Long):MongoDBObject = {
      val builder = MongoDBObject.newBuilder
      for ((k, v) <- properties) {
        builder += k -> convertHistory(v.compressAndReturnOldHistory(cutOff))
      }
      builder.result
  }

  def retrieveVertexHistory(id:Long):SavedHistory = {
    parse(vertices.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0,"history" -> 1)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")).extract[SavedHistory]
  }
  def retrieveVertexPropertyHistory(id:Long,key:String):SavedProperty ={
    val json = vertices.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0,s"properties.$key" -> 1)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")
    parse(json.substring(17,json.length-1).replaceFirst(key,"property")).extract[SavedProperty]
  }
  def retrieveVertex(id:Long):SavedVertex ={
    parse(vertices.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")).extract[SavedVertex]
  }
  def retrieveVertexRaw(id:Long):String = {
    vertices.findOne(MongoDBObject("_id" -> id), MongoDBObject("_id" -> 0)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")
  }

  def retrieveEdgeHistory(id:Long):SavedHistory = {

    parse(edges.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0,"history" -> 1)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")).extract[SavedHistory]
  }

  def retrieveEdgePropertyHistory(id:Long,key:String):SavedProperty ={
    val json = edges.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0,s"properties.$key" -> 1)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")
    parse(json.substring(17,json.length-1).replaceFirst(key,"property")).extract[SavedProperty]
  }

  def retrieveEdge(id:Long):SavedEdge ={
    parse(edges.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")).extract[SavedEdge]
  }
  def retrieveEdgeRaw(id:Long):String ={
    edges.findOne(MongoDBObject("_id" -> id),MongoDBObject("_id"->0)).getOrElse("").toString.toString.replaceAll("""\{ "\$numberLong" : "([0-9]*)" \}""", "$1")
  }

}
case class SavedVertex(history:List[HistoryPoint],properties:Option[Map[String,List[PropertyPoint]]],oldestPoint:Long,associatedEdges:Option[List[Long]])
case class SavedEdge(history:List[HistoryPoint],properties:Option[Map[String,List[PropertyPoint]]])
case class SavedHistory(history:List[HistoryPoint])
case class SavedProperties(properties:Map[String,List[PropertyPoint]])
case class SavedProperty(property:List[PropertyPoint])
case class PropertyPoint(time:Long, value: String)
case class HistoryPoint(time:Long,value:Boolean)
case class MongoLong($numberLong:Long)



//println(vertices.findOne(("_id" $eq id),MongoDBObject("_id"->0,s"properties.$key"->1)).getOrElse(""))
//println(vertices.findOne(MongoDBObject("_id" -> id,"properties" -> "properties" $elemMatch(MongoDBObject("name" -> "key"))),MongoDBObject("_id"->0,s"properties.$key" -> 1)).getOrElse(""))

//convertProperties(builder,entity.properties,cutOff) // no outside properties list
