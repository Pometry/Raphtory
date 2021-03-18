package com.raphtory.dev.allcommands

import java.net.InetAddress

import com.mongodb.casbah.MongoClientURI
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object ResultsCompare extends App{
  if(args.length<4) {
    println("Please provide analyser job ID's to compare")
    System.exit(1)
  }
  stateCheck(args(0),args(1))
  connectedComponentsCheck(args(2),args(3))



  //val toCompare = Await.result(database.getCollection(args(1)).find().toFuture(), Duration.Inf)
  def stateCheck(baseCollectionID:String,compareCollectionID:String) = {
    val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim
    val codecRegistry = fromRegistries(fromProviders(classOf[StateCheckResult]), DEFAULT_CODEC_REGISTRY )
    val mongo = MongoClient()
    val database: MongoDatabase = mongo.getDatabase("raphtory").withCodecRegistry(codecRegistry)

    val baseCollection: MongoCollection[StateCheckResult] = database.getCollection(baseCollectionID)
    val base = Await.result(baseCollection.find().toFuture(), Duration.Inf).map(state=>(TimeParams(state.time,state.window),state)).toMap

    val compareCollection: MongoCollection[StateCheckResult] = database.getCollection(compareCollectionID)
    val compare = Await.result(compareCollection.find().toFuture(), Duration.Inf).map(state=>(TimeParams(state.time,state.window),state)).toMap

    val stateResult = base.map(baseValue=> baseValue._2.compareTo(compare(baseValue._1)))
    if(stateResult.fold(true){(x,y) =>x&&y})
      println("The state of both these graphs is the same")
    else{
      val incorrectResults =  base.map(baseValue=> (baseValue._1,baseValue._2.compareTo(compare(baseValue._1)))).filter(x=>x._2==false).map(x=>x._1).toList
      println("The state of these graphs is incorrect, the following time params were found to not match:")
      println(incorrectResults)
    }

    val timeResults = base.map(baseValue=> (((compare(baseValue._1).viewTime)-baseValue._2.viewTime).toDouble/baseValue._2.viewTime)*100)
    val meanPercent = timeResults.sum/timeResults.size
    if(meanPercent<0)
      println(s"The second state run was on average $meanPercent% slower than the first")
    else
      println(s"The second state run was on average $meanPercent% faster than the first")

  }

  def connectedComponentsCheck(baseCollectionID:String,compareCollectionID:String) ={
    val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim
    val codecRegistry = fromRegistries(fromProviders(classOf[ConnectedComponentsResults]), DEFAULT_CODEC_REGISTRY )
    val mongo = MongoClient()
    val database: MongoDatabase = mongo.getDatabase("raphtory").withCodecRegistry(codecRegistry)

    val baseCollection: MongoCollection[ConnectedComponentsResults] = database.getCollection(baseCollectionID)
    val base = Await.result(baseCollection.find().toFuture(), Duration.Inf).map(state=>(TimeParams(state.time,state.windowsize),state)).toMap

    val compareCollection: MongoCollection[ConnectedComponentsResults] = database.getCollection(compareCollectionID)
    val compare = Await.result(compareCollection.find().toFuture(), Duration.Inf).map(state=>(TimeParams(state.time,state.windowsize),state)).toMap

    val stateResult = base.map(baseValue=> baseValue._2.compareTo(compare(baseValue._1)))
    if(stateResult.fold(true){(x,y) =>x&&y})
      println("The connected components of both these graphs is the same")
    else{
      val incorrectResults =  base.map(baseValue=> (baseValue._1,baseValue._2.compareTo(compare(baseValue._1)))).filter(x=>x._2==false).map(x=>x._1).toList
      println("The connected components of these graphs is incorrect, the following time params were found to not match:")
      println(incorrectResults)
    }

    val timeResults = base.map(baseValue=> (((compare(baseValue._1).viewTime)-baseValue._2.viewTime).toDouble/baseValue._2.viewTime)*100)
    val meanPercent = timeResults.sum/timeResults.size
    if(meanPercent<0)
      println(s"The second connected components run was on average $meanPercent% slower than the first")
    else
      println(s"The second connected components run was on average $meanPercent% faster than the first")

  }

}
case class TimeParams(time:Long,window:Long)
case class StateCheckResult(time:Long,window:Long,viewTime:Long,vertices:Long,maxDeg:Long,totalInEdges:Long,totalOutEdges:Long,vdeletionstotal:Long,vcreationstotal:Long,
                            outedgedeletionstotal:Long,outedgecreationstotal:Long,inedgedeletionstotal:Long,inedgecreationstotal:Long,
                            properties:Long,propertyhistory:Long,outedgeProperties:Long,outedgePropertyHistory:Long,inedgeProperties:Long,inedgePropertyHistory:Long){
  def compareTo(compare:StateCheckResult):Boolean = {
    vertices==compare.vertices &&
      maxDeg==compare.maxDeg &&
      totalInEdges==compare.totalInEdges &&
      totalOutEdges==compare.totalOutEdges &&
      vdeletionstotal==compare.vdeletionstotal &&
      vcreationstotal==compare.vcreationstotal &&
      outedgedeletionstotal==compare.outedgedeletionstotal &&
      outedgecreationstotal==compare.outedgecreationstotal &&
      inedgedeletionstotal==compare.inedgedeletionstotal &&
      inedgecreationstotal==compare.inedgecreationstotal &&
      properties==compare.properties &&
      propertyhistory==compare.propertyhistory &&
      outedgeProperties==compare.outedgeProperties &&
      outedgePropertyHistory==compare.outedgePropertyHistory &&
      inedgeProperties==compare.inedgeProperties &&
      inedgePropertyHistory==compare.inedgePropertyHistory

  }
}
case class ConnectedComponentsResults(time:Long,windowsize:Long,viewTime:Long,top5:List[Long],total:Long,totalIslands:Long,proportion:Double,clustersGT2:Long){
  def compareTo(compare:ConnectedComponentsResults):Boolean = {
    top5 == compare.top5 &&
      total == compare.total &&
      totalIslands == compare.totalIslands &&
      proportion == compare.proportion &&
      clustersGT2 == compare.clustersGT2

  }

}