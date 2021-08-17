package com.raphtory.resultcomparison

import spray.json.DefaultJsonProtocol

object comparisonJsonProtocol extends DefaultJsonProtocol {
  implicit val stateCheck = jsonFormat19(StateCheckResult)
}
case class TimeParams(time:Long,window:Long)
case class StateCheckResult(time:Long,windowsize:Long,viewTime:Long,vertices:Long,maxDeg:Long,totalInEdges:Long,totalOutEdges:Long,vdeletionstotal:Long,vcreationstotal:Long,
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
case class ConnectedComponentsResults(time:Long,windowsize:Long,viewTime:Long,top5:List[Long],total:Long,totalIslands:Long,proportion:Double,clustersGT2:Long) {
  def compareTo(compare: ConnectedComponentsResults): Boolean = {
    top5 == compare.top5 &&
      total == compare.total &&
      totalIslands == compare.totalIslands &&
      proportion == compare.proportion &&
      clustersGT2 == compare.clustersGT2
  }
}