package com.raphtory.examples.bitcoin.actors;

import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.random.analysis.TestAnalyser;

class BitcoinLiveAnalysisManager extends LiveAnalysisManager {

    override protected def defineMaxSteps(): Int = 10

    override protected def generateAnalyzer: Analyser = new TestAnalyser()

    override protected def processResults(result: Any): Unit = println(result)

    override protected def processOtherMessages(value: Any): Unit = ""

}
