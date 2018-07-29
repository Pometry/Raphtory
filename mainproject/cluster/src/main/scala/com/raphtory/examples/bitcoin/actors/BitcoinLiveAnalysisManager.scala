package com.raphtory.examples.bitcoin.actors;

import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.bitcoin.analysis.BitcoinAnalyser

class BitcoinLiveAnalysisManager extends LiveAnalysisManager {

    override protected def defineMaxSteps(): Int = 1

    override protected def generateAnalyzer: Analyser = new BitcoinAnalyser()

    override protected def processResults(result: Any): Unit = {
        val results:Vector[Vector[(String, Double)]] = result.asInstanceOf[(Vector[Vector[(String, Double)]]) ]
        var finalResults = Vector.empty[(String, Double)]
        for(indiResult <- results){
            for (pair <- indiResult){
               finalResults :+= pair
            }
        }
        println("Current top three wallets")
        finalResults.sortBy(f => f._2)(Ordering[Double].reverse).take(3).foreach(pair =>{
            println(s"${pair._1} has acquired a total of ${pair._2} bitcoins ")
        })

    }

    override protected def processOtherMessages(value: Any): Unit = ""

}
