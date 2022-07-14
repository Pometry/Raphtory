package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.raphtory.utils.FileUtils

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.io.Source
import scala.reflect.runtime.universe._
import scala.util.Random
import scala.util.matching.Regex

/** A [[com.raphtory.api.input.Spout Spout]] that reads files from disk.
  *
  * This Spout takes a given file or folder and reads the contents line by line passing these to the graph builders.
  * If a file is given, only this will be read, if a folder is provided, all included files fitting the regexPattern will be read.
  *
  * As a more advanced feature, the Spout has a generic T type which in combination with the optional lineConverter (String => T) parameter allows each line to be
  * converted into any serialisable type the user desires as long as a suitable GraphBuilder[T] is provided when building the graph.
  *
  * @param path The path to the directory or file the user is interested in reading from.
  * @param regexPattern A regex pattern used to select only certain files in a folder. Defaults to .csv.
  * @param reReadFiles A boolean flag specifying if the user wishes the spout to reread the file once it has finished. This is useful for updated log files and monitoring folders where new data is periodically dumped into.
  * @param recurse A boolean flag for if the user wants the spout to recursively read all subdirectories in a given directory.
  * @param lineConverter If the FileSpout is intended to output anything other than Strings the user must specify how to make the conversion.
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.FileSink
  * import com.raphtory.spouts.FileSpout
  *
  * val fileSpout = new FileSpout("/path/to/your/data")
  * val graph = Raphtory.load(fileSpout, YourGraphBuilder())
  * val sink = FileSink("/tmp/raphtoryTest")
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[com.raphtory.api.input.Spout Spout]]
  *      [[com.raphtory.Raphtory Raphtory]]
  */
class FileSpout[T: TypeTag](
    val path: String = "",
    val regexPattern: String = "^.*\\.([cC][sS][vV]??)$",
    val reReadFiles: Boolean = false,
    val recurse: Boolean = false,
    val lineConverter: (String => T)
) extends Spout[T] {
  private val completedFiles: mutable.Set[String] = mutable.Set.empty[String]

  private val outputDirectory = "/tmp/raphtory-file-spout/" + Random.nextLong()

  private var inputPath = Option(path).filter(_.trim.nonEmpty).getOrElse("/tmp")
  // If the inputPath is not an absolute path then make an absolute path
  if (!new File(inputPath).isAbsolute)
    inputPath = new File(inputPath).getAbsolutePath

  private val fileRegex = new Regex(regexPattern)

  private var files             = getMatchingFiles()
  private var filesToProcess    = extractFilesToIngest()
  private var currentfile: File = _

  private var lines = files.headOption match {
    case Some(file) =>
      files = files.tail
      processFile(file)
    case None       => Iterator[String]()
  }

  override def hasNext: Boolean =
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
        //processingErrorCount.inc()
        throw ex
    }

  def processFile(file: File) = {
    logger.info(s"Spout: Processing file '${file.toPath.getFileName}' ...")

    val fileName = file.getPath.toLowerCase
    currentfile = file
    val source   = fileName match {
      case name if name.endsWith(".gz")  =>
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(file.getPath)))
      case name if name.endsWith(".zip") =>
        Source.fromInputStream(new ZipInputStream(new FileInputStream(file.getPath)))
      case _                             => Source.fromFile(file)
    }

    //processedFiles.inc()
    try source.getLines()
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        //processingErrorCount.inc()
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
            .createSymbolicLink(
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

  override def nextIterator(): Iterator[T] =
    if (typeOf[T] =:= typeOf[String]) lines.asInstanceOf[Iterator[T]]
    else lines.map(lineConverter)

}

object FileSpout {

  def apply[T: TypeTag](
      source: String,
      regexPattern: String,
      reReadFiles: Boolean,
      recurse: Boolean,
      lineConverter: (String => T)
  ) =
    new FileSpout[T](source, regexPattern, reReadFiles, recurse, lineConverter)

  def apply(
      source: String = "",
      regexPattern: String = "^.*\\.([cC][sS][vV]??)$",
      reReadFiles: Boolean = false,
      recurse: Boolean = false
  ) =
    new FileSpout[String](source, regexPattern, reReadFiles, recurse, lineConverter = s => s)
}

//TODO work out how to get the correct deployment ID here

//  val deploymentID: String = conf.getString("raphtory.deploy.id")
// Validate that the path exists and is readable
// Throws exception or logs error in case of failure
// FileUtils.validatePath(inputPath) // TODO Change this to cats.Validated
//private val processingErrorCount =
//  ComponentTelemetryHandler.fileProcessingErrors.labels(deploymentID)
// private val processedFiles       = ComponentTelemetryHandler.filesProcessed.labels(deploymentID)
