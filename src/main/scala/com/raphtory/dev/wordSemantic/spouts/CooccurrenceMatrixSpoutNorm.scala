//package examples.wordSemantic.spouts
//
//import scala.io.Source
//
//class CooccurrenceMatrixSpoutNorm extends CooccurrenceMatrixSpoutFiltered {
//
//  var mapSum = Map(""->0L)
//
//  override def scaling(): Double = {
//    val fname = filename.split("/").last
//    try{
//      mapSum(fname)
//    }catch{
//      case e: Exception =>
//        val sumFreq = System.getenv().getOrDefault("WS_SUM_FREQ_FILE", "/home/tsunade/qmul/datasets/word_semantics/test/ws-month-data-2000-fth/test-res.csv").trim
//        mapSum = getSumPerFile(sumFreq)
//        mapSum(fname)
//    }
//  }
//
//  def getSumPerFile(file: String): Map[String, Long] = {
//    val fil = Source.fromFile(file)
//    val lines = fil.getLines
//    fil.close()
//    lines.map(_.split(",").map(_.trim)).map(f=>f(0)->f(1).toLong).toMap
//  }
//}
