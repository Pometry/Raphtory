package com.raphtory.resultcomparison

import spray.json.DefaultJsonProtocol

object comparisonJsonProtocol extends DefaultJsonProtocol {
  implicit val stateCheck = jsonFormat18(StateCheckResult)
  implicit val connectedComponentsResults = jsonFormat8(ConnectedComponentsResults)
}

case class TimeParams(time:Long,window:Long)

abstract class RaphtoryResultComparitor[T](time:Long,windowsize:Long) {
  def compareTo(compare: T): Boolean
}

case class StateCheckResult(val time:Long, val windowsize:Long, viewTime:Long, vertices:Long, totalInEdges:Long, totalOutEdges:Long, vdeletionstotal:Long, vcreationstotal:Long,
                            outedgedeletionstotal:Long, outedgecreationstotal:Long, inedgedeletionstotal:Long, inedgecreationstotal:Long,
                            properties:Long, propertyhistory:Long, outedgeProperties:Long, outedgePropertyHistory:Long, inedgeProperties:Long, inedgePropertyHistory:Long)
                            extends  RaphtoryResultComparitor[StateCheckResult](time, windowsize) {
  def compareTo(compare: StateCheckResult):Boolean = {
    val x = this.vertices == compare.vertices &&
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

    if(!x)
      println(this + "   "+ compare)
    x
  }


}

case class ConnectedComponentsResults(val time:Long, val windowsize:Long, viewTime:Long, top5:List[Long], total:Long, totalIslands:Long, proportion:Double, clustersGT2:Long)
                                      extends RaphtoryResultComparitor[ConnectedComponentsResults](time, windowsize) {
  def compareTo(compare: ConnectedComponentsResults): Boolean = {
    top5 == compare.top5 &&
      total == compare.total &&
      totalIslands == compare.totalIslands &&
      proportion == compare.proportion &&
      clustersGT2 == compare.clustersGT2
  }
}