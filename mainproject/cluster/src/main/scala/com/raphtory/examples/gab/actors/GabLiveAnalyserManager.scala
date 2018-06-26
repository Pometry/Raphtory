package com.raphtory.examples.gab.actors

import com.raphtory.core.actors.analysismanager.LiveAnalyser
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gab.analysis.{GabPageRank, GabPageRank2, GabPageRank3}

class GabLiveAnalyserManager extends LiveAnalyser {
  /*private val B       : Int   = 100 // TODO set
  private val epsilon : Float = 0.85F
  private val delta1  : Float = 1F*/

  private val epsilon        = 0.00000001F
  private val dumplingFactor = 0.85F

  override protected def processResults(result: Any): Unit = println(
    result.asInstanceOf[Vector[Vector[(Long, Double)]]]
      .flatMap(e => e).sortBy(f => f._2)(Ordering[Double])
      .reverse
  )

  override protected def defineMaxSteps(): Int = {
    //steps =  (B * Math.log(getNetworkSize/epsilon)).round
    100 //Int.MaxValue
  }

  override protected def generateAnalyzer : Analyser = new GabPageRank3(getNetworkSize, dumplingFactor)
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd(result: Any): Boolean = {
    val _newResults = results.asInstanceOf[Vector[Vector[(Long, Double)]]].flatMap(v => v)
    val _oldResults = oldResults.asInstanceOf[Vector[Vector[(Long, Double)]]].flatMap(v => v)
    println(s"newResults: ${_newResults.size} => ${_oldResults.size}")
    if (_oldResults.size < 1 && _newResults.size > 0) {
      println("OldResults are empty, newResults were filled")
      return false // Process has not finished (just first iteration)
    }

    return false
    if (_newResults.size < 1) {
      println("NewResults are empty.")
      return true // Graph is now empty (endProcess/retry in a while)
    }

    val newSum = _newResults.sum(resultNumeric)._2
    val oldSum = _oldResults.sum(resultNumeric)._2

    println(s"newSum = $newSum - oldSum = $oldSum - diff = ${newSum - oldSum}")
    results = _newResults
    Math.abs(newSum - oldSum) / _newResults.size < epsilon
  }

  implicit object resultNumeric extends Numeric[(Long, Double)] {
    override def plus(x: (Long, Double), y: (Long, Double)) = (x._1 + y._1, x._2 + y._2)
    override def minus(x: (Long, Double), y: (Long, Double)) = (x._1 - y._1, x._2 - y._2)
    override def times(x: (Long, Double), y: (Long, Double)) = (x._1 * y._1, x._2 * y._2)
    override def negate(x: (Long, Double)) = (-x._1, -x._2)
    override def fromInt(x: Int) = (x, x)
    override def toInt(x: (Long, Double)) = x._1.toInt
    override def toLong(x: (Long, Double)) = x._1
    override def toFloat(x: (Long, Double)) = x._2.toFloat
    override def toDouble(x: (Long, Double)) = x._2
    override def compare(x: (Long, Double), y: (Long, Double)) = x._2.compare(y._2)
  }
}
