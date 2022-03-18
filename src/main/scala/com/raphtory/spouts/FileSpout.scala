package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout
import com.raphtory.core.deploy.Raphtory
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.util.FileUtils
import com.raphtory.core.config.Telemetry

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex
import scala.reflect.runtime.universe._

class FileSpout[T: TypeTag](val path: String = "", val lineConverter: (String => T), conf: Config)
        extends Spout[T] {
  private val completedFiles: mutable.Set[String] = mutable.Set.empty[String]
  val logger: Logger                              = Logger(LoggerFactory.getLogger(this.getClass))

  private val reReadFiles     = conf.getBoolean("raphtory.spout.file.local.reread")
  private val recurse         = conf.getBoolean("raphtory.spout.file.local.recurse")
  private val regexPattern    = conf.getString("raphtory.spout.file.local.fileFilter")
  private val sourceDirectory = conf.getString("raphtory.spout.file.local.sourceDirectory")
  // TODO HARDLINK wont work on a network share
  private val outputDirectory = conf.getString("raphtory.spout.file.local.outputDirectory")

  private var inputPath = Option(path).filter(_.trim.nonEmpty).getOrElse(sourceDirectory)
  // If the inputPath is not an absolute path then make an absolute path
  if (!new File(inputPath).isAbsolute)
    inputPath = new File(inputPath).getAbsolutePath

  private val fileRegex = new Regex(regexPattern)

  // Validate that the path exists and is readable
  // Throws exception or logs error in case of failure
  FileUtils.validatePath(inputPath) // TODO Change this to cats.Validated

  var files             = getMatchingFiles()
  var filesToProcess    = extractFilesToIngest()
  var currentfile: File = _

  var lines = files.headOption match {
    case Some(file) =>
      files = files.tail
      processFile(file)
    case None       => Iterator[String]()
  }

  override def hasNext(): Boolean =
    if (lines.hasNext)
      true
    else {
      // Add file to tracker so we do not read it again
      if (!reReadFiles)
        if (currentfile != null) {
          val fileName = currentfile.getPath.replace(outputDirectory, "")
          logger.debug(s"Spout: Adding file $fileName to completed list.")
          completedFiles.add(fileName)
        }
      lines = files.headOption match {
        case Some(file) =>
          files = files.tail
          processFile(file)
        case None       => Iterator[String]()
      }
      if (lines.hasNext)
        true
      else
        false
    }

  override def next(): T =
    try lineConverter(lines.next())
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        throw ex
    }

  private def processFile(file: File) = {
    logger.info(s"Spout: Processing file '${file.toPath.getFileName}' ...")
    Telemetry.totalFilesProcessed.inc()

    val fileName = file.getPath.toLowerCase
    currentfile = file
    val source   = fileName match {
      case name if name.endsWith(".gz")  =>
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(file.getPath)))
      case name if name.endsWith(".zip") =>
        Source.fromInputStream(new ZipInputStream(new FileInputStream(file.getPath)))
      case _                             => Source.fromFile(file)
    }

    try {
      val srcLines = source.getLines()
      val srcSize = srcLines.size
      Telemetry.totalLinesParsed.set(srcSize.toDouble)
      srcLines
    }
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        source.close()

        // Remove hard-link
        FileUtils.deleteFile(file.toPath)
        throw ex
    }
  }

  private def getMatchingFiles() =
    FileUtils.getMatchingFiles(inputPath, regex = fileRegex, recurse = recurse)

  private def checkFileName(file: File): String = //TODO: haaroon to fix
    if (new File(inputPath).getParent == "/")
      file.getPath
    else
      file.getPath.replace(new File(inputPath).getParent, "")

  private def extractFilesToIngest() =
    if (files.nonEmpty) {
      val tempDirectory = FileUtils.createOrCleanDirectory(outputDirectory)

      // Remove any files that has already been processed
      files = files.collect {
        case file if !completedFiles.contains(checkFileName(file)) =>
          logger.debug(
                  s"Spout: Found a new file '${file.getPath.replace(new File(inputPath).getParent, "")}' to process."
          )
          // mimic sub dir structure of files
          val sourceSubFolder = {
            val parentPath = new File(inputPath).getParent
            if (parentPath == "/")
              tempDirectory.getPath + file.getParent
            else
              tempDirectory.getPath + file.getParent.replace(new File(inputPath).getParent, "")
          }
          FileUtils.createOrCleanDirectory(sourceSubFolder, false)
          // Hard link the files for processing
          logger.debug(s"Spout: Attempting to hard link file '$file' -> '${Paths
            .get(sourceSubFolder + "/" + file.getName)}'.")
          try Files
            .createLink(
                    Paths.get(sourceSubFolder + "/" + file.getName),
                    file.toPath
            )
            .toFile
          catch {
            case ex: Exception =>
              logger.error(
                      s"Spout: Failed to hard link file ${file.getPath}, error: ${ex.getMessage}."
              )
              throw ex
          }
      }.sorted
    }
    else
      List[File]()

  override def spoutReschedules(): Boolean = true

  override def executeReschedule(): Unit = {
    files = getMatchingFiles()
    filesToProcess = extractFilesToIngest()
    lines = files.headOption match {
      case Some(file) =>
        files = files.tail
        processFile(file)
      case None       => Iterator[String]()
    }
  }

  override def hasNextIterator(): Boolean = hasNext()

  override def nextIterator(): Iterator[T] =
    if (typeOf[T] =:= typeOf[String]) lines.asInstanceOf[Iterator[T]]
    else lines.map(lineConverter)

  override def executeNextIterator(): Unit =
    for (line <- lines)
      try graphBuilder.parseTuple(lineConverter(line))
      catch {
        case ex: Exception =>
          logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
          throw ex
      }

}

object FileSpout {

  def apply[T: TypeTag](source: String, lineConverter: (String => T), config: Config) =
    new FileSpout[T](source, lineConverter, config)

  def apply(source: String = "") =
    new FileSpout[String](source, lineConverter = s => s, Raphtory.getDefaultConfig())
}
