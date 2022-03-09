package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout
import com.raphtory.core.deploy.Raphtory
import com.raphtory.util.FileUtils
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

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

class FileSpout[T](val path: String = "", val lineConverter: (String => T), conf: Config)
        extends Spout[T] {
  private val completedFiles: mutable.Set[String] = mutable.Set.empty[String]
  val logger: Logger                              = Logger(LoggerFactory.getLogger(this.getClass))

  private val reReadFiles     = conf.getBoolean("raphtory.spout.file.local.reread")
  private val recurse         = conf.getBoolean("raphtory.spout.file.local.recurse")
  private val regexPattern    = conf.getString("raphtory.spout.file.local.fileFilter")
  private val sourceDirectory = conf.getString("raphtory.spout.file.local.sourceDirectory")
  // TODO HARDLINK wont work on a network share
  private val outputDirectory = conf.getString("raphtory.spout.file.local.outputDirectory")

  private val inputPath = Option(path).filter(_.trim.nonEmpty).getOrElse(sourceDirectory)
  private val fileRegex = new Regex(regexPattern)

  // Validate that the path exists and is readable
  // Throws exception or logs error in case of failure
  FileUtils.validatePath(inputPath) // TODO Change this to cats.Validated

  var files =
    FileUtils.getMatchingFiles(inputPath, regex = fileRegex, recurse = recurse)

  var filesLeftToRead = true

  var filesToProcess = if (files.nonEmpty) {
    val tempDirectory = FileUtils.createOrCleanDirectory(outputDirectory)

    // Remove any files that has already been processed
    files.collect {
      case file if !completedFiles.contains(file.getPath.replace(inputPath, "")) =>
        logger.debug(
                s"Spout: Found a new file ${file.getPath.replace(inputPath, "")} to process."
        )
        // mimic sub dir structure of files
        val sourceSubFolder = tempDirectory.getPath + file.getParent.replace(inputPath, "")
        FileUtils.createOrCleanDirectory(sourceSubFolder, false)
        // Hard link the files for processing
        logger.debug(s"Spout: Attempting to hard link file '$file'.")
        try Files.createLink(
                Paths.get(sourceSubFolder + "/" + file.getName),
                file.toPath
        )
        catch {
          case ex: Exception =>
            logger.error(
                    s"Spout: Failed to hard link file ${file.getPath}, error: ${ex.getMessage}."
            )
            throw ex
        }
    }.sorted
  }
  else {
    filesLeftToRead = false
    List[File]()
  }

  var lines = files.headOption match {
    case Some(file) =>
      files = files.tail
      processFile(file)
    case None       => Iterator[String]()
  }

  override def hasNext(): Boolean = filesLeftToRead

  override def next(): Option[T] =
    try if (lines.hasNext) {
      val data = Some(lineConverter(lines.next()))
      if (!lines.hasNext)
        filesLeftToRead = false
      data
    }
    else {
      lines = files.headOption match {
        case Some(file) =>
          files = files.tail
          processFile(file)
        case None       => Iterator[String]()
      }
      if (lines.hasNext)
        Some(lineConverter(lines.next()))
      else {
        filesLeftToRead = false
        None
      }
    }
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        throw ex
    }

  private def processFile(file: File) = {
    logger.info(s"Spout: Processing file '${file.toPath.getFileName}' ...")

    val fileName = file.getPath.toLowerCase
    val source   = fileName match {
      case name if name.endsWith(".gz")  =>
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(file.getPath)))
      case name if name.endsWith(".zip") =>
        Source.fromInputStream(new ZipInputStream(new FileInputStream(file.getPath)))
      case _                             => Source.fromFile(file)
    }

    try source.getLines()
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        source.close()

        // Add file to tracker so we do not read it again
        if (!reReadFiles) {
          val fileName = file.getPath.replace(outputDirectory, "")
          logger.debug(s"Spout: Adding file $fileName to completed list.")

          completedFiles.add(fileName)
        }

        // Remove hard-link
        FileUtils.deleteFile(file.toPath)
        throw ex
    }

  }

  override def reschedule(): Boolean = true
}

object FileSpout {

  def apply[T](source: String, lineConverter: (String => T), config: Config) =
    new FileSpout[T](source, lineConverter, config)

  def apply(source: String = "") =
    new FileSpout[String](source, lineConverter = s => s, Raphtory.getDefaultConfig())
}

//  private def rescheduleFilePoll(): Unit = {
//    val runnable = new Runnable {
//      override def run(): Unit = readFiles()
//    }
//
//    // TODO: Parameterise the delay
//    logger.debug("Spout: Scheduling to poll files again in 10 seconds.")
//    scheduler.scheduleOnce(10, TimeUnit.SECONDS, runnable)
//  }
