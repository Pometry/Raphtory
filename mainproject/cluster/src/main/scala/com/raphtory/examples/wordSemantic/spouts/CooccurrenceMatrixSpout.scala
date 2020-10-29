//package com.raphtory.examples.wordSemantic.spouts
//
//import java.time.LocalDateTime
//
//import com.raphtory.core.components.Spout.SpoutTrait
//
//import scala.concurrent.duration._
//import scala.io.Source
//import scala.language.postfixOps
//
//class CooccurrenceMatrixSpout extends SpoutTrait {
//
//  val file_name = System.getenv().getOrDefault("DATA_FILENAME", "/home/tsunade/qmul/datasets/word_semantics/test_file_0.txt").trim
//  val fl = Source.fromFile(file_name)
//  val fileLines = fl.getLines//.toArray
//  var time = file_name.replace(".txt", "").split('_').last
//  // upstream/master
////  var position    = 0
////  var linesNumber = fl.length
//  println("Starting File ingestion: " + LocalDateTime.now())
////  println("Lines :" + linesNumber)
//
//  override def ProcessSpoutTask(message: Any): Unit = message match {
//    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
//    case "newLine" =>
//
//     // if (position < linesNumber) {
//        //val line = if(fileLines.hasNext) fileLines.next() else ""
//        try {
//          sendTuple(time+' '+fileLines.next())
//          AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
//        }catch{
//          case e: NoSuchElementException => println("End of file!")
//        }
//    //    position += 1
//   //   }
//    case _ => println("message not recognized!")
//  }
//}