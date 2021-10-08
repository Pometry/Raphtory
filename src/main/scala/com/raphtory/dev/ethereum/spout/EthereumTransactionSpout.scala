package com.raphtory.dev.ethereum.spout

import com.raphtory.core.components.spout.Spout
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, WildcardFileFilter}
import org.apache.hadoop.conf.Configuration
import com.github.mjakubowski84.parquet4s.ParquetReader
import com.raphtory.dev.ethereum.EthereumTransaction

import java.io.File
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import scala.collection.mutable

class EthereumTransactionSpout extends Spout[EthereumTransaction] {

  val MAX_QUEUE_SIZE = 400000
  private val envDirectory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val directory = new File(envDirectory)
  val fileFilter = new WildcardFileFilter("*.parquet")
  val files = FileUtils listFiles(
    directory,
    fileFilter,
    DirectoryFileFilter.DIRECTORY
  )
  var filePaths = files.map { file => file.getAbsolutePath }
  filePaths = filePaths.toArray.sorted

  val fileQueue = mutable.Queue[EthereumTransaction]()
  var prevQueueSize = 0
  override def setupDataSource(): Unit = {}

  override def generateData(): Option[EthereumTransaction] = {
    //    if ((fileQueue.size %  1000) == 0){
    //      println("Spout: Queue has "+fileQueue.size.toString+" items remaining")
    //    }
    if (filePaths.size == 0) {
      dataSourceComplete()
      return None
    } else if (fileQueue.size < MAX_QUEUE_SIZE) {
      println("Spout: Files remaining " + filePaths.size.toString)
      //        println("Spout: Queue has "+fileQueue.size.toString+" items remaining")
      val nextFile = filePaths.take(1).head
      filePaths = filePaths.tail
      val parquetIterable = ParquetReader.read[EthereumTransaction](nextFile)
      parquetIterable.foreach { tx => fileQueue += tx }

    }
    println("Spout: Queue has "+fileQueue.size.toString+" items remaining")
    Some(fileQueue.dequeue())
  }

  override def closeDataSource():  Unit = {}
}
