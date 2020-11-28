package com.raphtory.examples.concirrus

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer

class BoatCoOcurrance(args: Array[String]) extends Analyser(args) {


  override def returnResults(): Any = {
    view.getVertices().filter(v => v.Type() equals ("Location")).map(loc => {
      loc.getIncEdges.map(e => { //We get all incoming edges
        val toCompare = loc.getIncEdges.filter(edge => edge != e) //then filter incoming edges to remove itself
        e.getHistory().flatMap(f => {
          //for each element in the history of the edge
          toCompare.filter(edge => //go through all other edges
            edge.getHistory()
              .exists(p => Math.abs(p._1 - f._1) <= 3600)) //and see if they contain a point +/- an hour
            .map(edge => edge.src()) //and if so return their ID
        })
      })
    })
  }

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = ???


  override def setup(): Unit = {}

  override def analyse(): Unit = {}


  override def defineMaxSteps(): Int = 1

}
