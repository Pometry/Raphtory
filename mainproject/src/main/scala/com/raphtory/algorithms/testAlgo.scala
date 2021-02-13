package com.raphtory.algorithms

import java.io._
import java.util

import org.apache.spark.sql.{Row, SparkSession}

object testAlgo extends App {



    //TODO work out loggging here
    //log.info("initialise FileSpout")
    private val directory = "/home/tsunade/qmul/datasets/word_semantics/tmp/sample-0.1"//System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
    private val fileName = ""//System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
    private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean

    private var fileManager = FileManager(directory, fileName, dropHeader)


      while (!fileManager.allCompleted) {
        val (newFileManager, line) = fileManager.nextLine()
        fileManager = newFileManager
        if (line!= Row.empty) println(line.getAs[Long](0))
      }
  println("done")


  }

  final case class FileManager private (
                                         currentFileReader: Option[util.Iterator[Row]],
                                         restFiles: List[File],
                                         dropHeader: Boolean
                                       )  {
    def nextFile(): FileManager = this.copy(currentFileReader = None)

    lazy val allCompleted: Boolean = currentFileReader.isEmpty && restFiles.isEmpty
    lazy val spark: SparkSession =   SparkSession.builder().master("local").getOrCreate()

    def nextLine(): (FileManager, Row) = currentFileReader match {
      case None =>
        restFiles match {
          case Nil => (this, Row.empty)
          case head :: tail =>
            val reader             = getFileReader(head)
            val (block, endOfFile) = readBlockAndIsEnd(reader)
            val currentReader      = if (endOfFile) None else Some(reader)
            (this.copy(currentFileReader = currentReader, restFiles = tail), block)
        }
      case Some(reader) =>
        val (block, endOfFile) = readBlockAndIsEnd(reader)
        if (endOfFile) (this.copy(currentFileReader = None), block)
        else (this, block)

    }

    private def readBlockAndIsEnd(reader: util.Iterator[Row]): (Row, Boolean) = {
      if (reader.hasNext)
        (reader.next(), false)
      else{
        (Row.empty ,true)
      }
    }

    private def getFileReader(file: File): util.Iterator[Row] = {
      println(s"Reading file ${file.getCanonicalPath}")
      val br = spark.read.parquet(file.getCanonicalPath)
      br.toLocalIterator()
    }
  }

  object FileManager {
    private val joiner     = System.getenv().getOrDefault("FILE_SPOUT_JOINER", "/").trim //gabNetwork500.csv
    def apply(dir: String, fileName: String, dropHeader: Boolean): FileManager = {
      val filesToRead =
        if (fileName.isEmpty)
          getListOfFiles(dir)
        else {
          val file = new File(dir + joiner + fileName)
          if (file.exists && file.isFile)
            List(file)
          else {
            println(s"File $dir$joiner$fileName does not exist or is not file ")
            List.empty
          }
        }
      FileManager(None, filesToRead, dropHeader)
    }

    private def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        val files = getRecursiveListOfFiles(d)
        files.filter(f => f.isFile && !f.isHidden && f.getName.endsWith(".parquet"))
      }
      else {
        println(s"Directory $dir does not exist or is not directory")
        List.empty
      }
    }

    private def getRecursiveListOfFiles(dir: File): List[File] = {
      val these = dir.listFiles.toList
      these ++ these.filter(_.isDirectory).sorted.flatMap(getRecursiveListOfFiles)
    }
  }
