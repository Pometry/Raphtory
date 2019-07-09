package com.raphtory.examples.bitcoin.actors;

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.bitcoin.analysis.BitcoinAnalyser
import com.raphtory.examples.bitcoin.communications.CoinsAquiredPayload

class BitcoinLiveAnalysisManager extends LiveAnalysisManager {
    override protected def defineMaxSteps(): Int = 1

    override protected def generateAnalyzer: Analyser = new BitcoinAnalyser()

    override protected def processResults(result: Any): Unit = {
        val results:Vector[CoinsAquiredPayload] = result.asInstanceOf[(Vector[CoinsAquiredPayload])]
        var finalResults = Vector.empty[(String, Double)]
        var highestBlock = 0
        var blockHash = ""
        for(indiResult <- results){
            for (pair <- indiResult.wallets){
               finalResults :+= pair
            }
            if(indiResult.highestBlock>highestBlock){
                highestBlock = indiResult.highestBlock
                blockHash = indiResult.blockhash
            }
        }
        println(s"Current top three wallets at block $highestBlock ($blockHash)")
        finalResults.sortBy(f => f._2)(Ordering[Double].reverse).take(3).foreach(pair =>{
            println(s"${pair._1} has acquired a total of ${pair._2} bitcoins ")
        })

    }

    override protected def processOtherMessages(value: Any): Unit = ""

}
