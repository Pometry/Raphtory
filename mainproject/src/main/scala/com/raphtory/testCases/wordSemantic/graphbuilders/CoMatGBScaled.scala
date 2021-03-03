//package com.raphtory.testCases.wordSemantic.graphbuilders
//
//import com.raphtory.core.actors.Router.GraphBuilder
//import com.raphtory.core.model.communication._
//
//class CoMatGBScaled extends GraphBuilder[String] {
//  val THR = System.getenv().getOrDefault("COOC_FREQ_THRESHOLD ", "0.05").trim.toDouble
//
//  override def parseTuple(tuple: String) = {
//    //println(record)
//    var dp =tuple.split(" ").map(_.trim)
//    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
//
//    try {
//      dp = dp.last.split("\t")
//      val scale = scalling(dp.drop(2).grouped(2).map(_.head.toInt).toArray)
//      val srcClusterId = assignID(dp.head)
//      val len = dp.length
//      sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = srcClusterId, Properties(StringProperty("Word", dp.head))))
//      for (i <- 1 until len by 2) {
//        if ((dp(i+1).toLong/scale) >= THR) {
//          val dstClusterId = assignID(dp(i))
//          val coocWeight = dp(i + 1).toLong
//
//          sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = dstClusterId, Properties(StringProperty("Word", dp(i)))))
//          sendUpdate(
//            EdgeAddWithProperties(
//              msgTime = occurenceTime,
//              srcID = srcClusterId,
//              dstID = dstClusterId,
//              Properties(LongProperty("Frequency", coocWeight),
//                  DoubleProperty("ScaledFreq", coocWeight/scale))
//            )
//          )
//        }
//      }
//    }catch {
//      case e: Exception => println(e, dp.length, tuple.asInstanceOf[String])
//    }
//  }
//    def scalling(freq: Array[Int]): Double = {
//     math.sqrt(freq.map(math.pow(_, 2)).sum)
//    }
//}
