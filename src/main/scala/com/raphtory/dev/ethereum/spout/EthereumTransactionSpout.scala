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

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[EthereumTransaction] = {
    if (filePaths.size == 0){
      dataSourceComplete()
    } else {
        val nextFile = filePaths.take(1).head
        filePaths = filePaths.tail
        val parquetIterable = ParquetReader.read[EthereumTransaction](nextFile)
        fileQueue ++= parquetIterable
        parquetIterable.close()
      }
    Some(fileQueue.dequeue())
  }

  override def closeDataSource():  Unit = {}
}
