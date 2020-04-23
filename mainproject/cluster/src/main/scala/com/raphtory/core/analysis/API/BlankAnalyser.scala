package com.raphtory.core.analysis.API

  import scala.collection.mutable.ArrayBuffer
  import com.raphtory.core.analysis.API.Analyser
  class BlankAnalyser(args:Array[String]) extends Analyser(args) {
    override def analyse(): Unit = {}
    override def setup(): Unit = {}
    override def returnResults(): Any = {}
    override def defineMaxSteps(): Int = 1
    override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {println("howdy!")}
  }

