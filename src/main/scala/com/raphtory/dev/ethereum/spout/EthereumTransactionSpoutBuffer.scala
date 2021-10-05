package com.raphtory.dev.ethereum.spout

import com.raphtory.core.components.spout.Spout
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, WildcardFileFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


class EthereumTransactionSpoutBuffer extends Spout[GenericRecord] {

  private val envDirectory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val directory = new File(envDirectory)
  //val directory = new File("/Users/Haaroony/OneDrive/OneDrive - University College London/PhD/Projects/scala/raphtory-ethereum2/resources/transactions_small")
  // val directory = new File("/Users/Haaroony/OneDrive/OneDrive - University College London/PhD/Projects/scala/raphtory-ethereum2/raphtory-ethereum/src/main/resources/transactions")
  val fileFilter = new WildcardFileFilter("*.parquet")
  val files = FileUtils listFiles(
    directory,
    fileFilter,
    DirectoryFileFilter.DIRECTORY
  )
  var filePaths = files.map { file => file.getAbsolutePath }
  filePaths = filePaths.toArray.sorted

  // avro
  val conf: Configuration = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  //  // get first item
  val nextFile = filePaths.take(1).head
  filePaths = filePaths.tail

  //val inFile: InputFile = HadoopInputFile.fromPath(new Path(nextFile), new Configuration())
  //var parquetReader = AvroParquetReader.builder[GenericRecord](inFile).withConf(conf).build()
  //var parquetReader = ParquetReader.builder(new Path(nextFile)).build()
  //var parquetReader = ParquetReader.builder(new AvroReadSupport[GenericRecord](), new Path(nextFile)).build()
  var inputFile = HadoopInputFile.fromPath(new Path(nextFile), conf)
  var parquetReader = AvroParquetReader.builder[GenericRecord](inputFile).withConf(conf).build()

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[GenericRecord] = {
    val conf: Configuration = new Configuration()
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    // get first element and repeat
    if (filePaths.size == 0){
      dataSourceComplete()
      None
    } else {
      var line = parquetReader.read()
      // if line is null then get the next file
      if (line == null){
        // parquetReader.close()
        // pop the last file
        val nextFile = filePaths.take(1).head
        filePaths = filePaths.tail
        //val parquetPath: Path = new Path(nextFile)
        // val inFile: InputFile = HadoopInputFile.fromPath(new Path(nextFile), new Configuration())
        // parquetReader = AvroParquetReader.builder[GenericRecord](inFile).withConf(conf).build()
        parquetReader = AvroParquetReader.builder[GenericRecord](HadoopInputFile.fromPath(new Path(nextFile), conf)).withConf(conf).build()
        // get its line
        line = parquetReader.read()
      }
      Some(line)
    }
  }
  override def closeDataSource(): Unit = {
  }
}
