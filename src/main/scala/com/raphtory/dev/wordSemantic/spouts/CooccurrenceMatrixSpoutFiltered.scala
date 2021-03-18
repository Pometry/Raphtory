//package examples.wordSemantic.spouts
//
//
//import java.io.{BufferedReader, File}
//
//import com.raphtory.core.actors.Spout.Spout
//import com.typesafe.scalalogging.LazyLogging
//import examples.wordSemantic.spouts.CooccurrenceMatrixSpout
//
//class CooccurrenceMatrixSpoutFiltered extends CooccurrenceMatrixSpout {
//
//  case class Filet( override val currentFileReader: Option[BufferedReader],
//                    override val restFiles: List[File],
//                    override val dropHeader: Boolean,
//                    override val timeInc: Long,
//                    scale: Double
//                  ) extends FileManager(currentFileReader,restFiles, dropHeader, timeInc)  {
//
//    override def nextLine(): (FileManager, String) = currentFileReader match {
//      case None =>
//        restFiles match {
//          case Nil => (this, "")
//          case head :: tail =>
//            val reader             = getFileReader(head)
//            val (block, endOfFile) = readBlockAndIsEnd(reader)
//            val currentReader      = if (endOfFile) None else Some(reader)
//            val time = head.getName.split('/').last.stripPrefix("D-").stripSuffix("_merge_occ").toLong * 1000000000L
//            val scale = scalling(block.split('\t').drop(2).grouped(2).map(_.head.toInt).toArray)
//            (this.copy(currentFileReader = currentReader, restFiles = tail,timeInc=time+1), (timeInc+1).toString +' '+scale+' '+ block)
//        }
//      case Some(reader) =>
//        val (block, endOfFile) = readBlockAndIsEnd(reader)
//        if (endOfFile) (this.copy(currentFileReader = None, timeInc=0L), block)
//        else (this.copy(timeInc=timeInc+1), timeInc.toString +' '+block)
//
//    }
//  }
//
////package com.raphtory.examples.wordSemantic.spouts
////
////import java.io.{BufferedReader, File, FileReader}
////import java.time.LocalDateTime
////
////import com.raphtory.core.components.Spout.Spout
////import com.raphtory.core.model.communication.StringSpoutGoing
////
////import scala.concurrent.duration._
////import scala.language.postfixOps
////
////class CooccurrenceMatrixSpoutFiltered extends CooccurrenceMatrixSpout {
//// var freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
////  var scale = scalling(freq)
////
////  override def nextLineSlice() = {
////    try {
////      if (posSlice <= currentLine.length-1) {
////        val head = currentLine(0)
////        for (i<- 1 to Set(JUMP, currentLine.length-posSlice/JUMP2).min) {
////          val currentSlice = currentLine.slice(posSlice, posSlice + JUMP2)
////          sendTuple(StringSpoutGoing(cnt.toString + ' ' + scale.toString + ' ' + head + "\t" + currentSlice.mkString("\t")))
////          posSlice += JUMP2
////        }
////        AllocateSpoutTask(Duration(1, MILLISECONDS), "nextLineSlice")
////      }
////      else {
////        posSlice = 1
////        AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
////      }
////    }catch {
////      case e: Exception => println(e,  posSlice)
////    }
////  }
////
////  override def nextLineBlock() = {
////    try {
////      cnt += 1
////      cline = currentFile.readLine()
////      currentLine = cline.split("\t")
////      freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
////      scale = scalling(freq)
////      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineSlice")
////      }
////    catch {
////      case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
////    }
////  }
////
//  def scalling(freq: Array[Int]): Double = {
//   math.sqrt(freq.map(math.pow(_, 2)).sum)
//  }
//}