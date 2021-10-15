package com.raphtory.spouts

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetRecordDecoder}
import com.raphtory.core.components.spout.Spout
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, WildcardFileFilter}

import java.io.File
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class ParquetSpout[T](directory:String)(implicit pr: ParquetRecordDecoder[T]) extends Spout[T] {

  val MAX_QUEUE_SIZE = 400000
  private val dir = new File(directory)
  val fileFilter = new WildcardFileFilter("*.parquet")
  val files = FileUtils listFiles(
    dir,
    fileFilter,
    DirectoryFileFilter.DIRECTORY
  )
  var filePaths = files.map { file => file.getAbsolutePath }
  filePaths = filePaths.toArray.sorted

  val dataQueue = mutable.Queue[T]()
  var prevQueueSize = 0
  override def setupDataSource(): Unit = {}

  override def generateData(): Option[T] = {
    //    if ((fileQueue.size %  1000) == 0){
    //      println("Spout: Queue has "+fileQueue.size.toString+" items remaining")
    //    }
    if (filePaths.size == 0 && dataQueue.size == 0) {
      dataSourceComplete()
      return None
    } else if (dataQueue.size < MAX_QUEUE_SIZE && filePaths.size != 0) {
      //      println("Spout: Files remaining " + filePaths.size.toString)
      //        println("Spout: Queue has "+fileQueue.size.toString+" items remaining")
      val nextFile = filePaths.take(1).head
      filePaths = filePaths.tail
      val parquetIterable = ParquetReader.read[T](nextFile)
      parquetIterable.foreach { tx => dataQueue += tx }
    }
    Some(dataQueue.dequeue)
  }

  override def closeDataSource():  Unit = {}
}
