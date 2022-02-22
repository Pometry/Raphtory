package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.BatchReceivePolicy
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.api.SubscriptionType

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file._
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.collection.JavaConverters._
import scala.collection.mutable.Set
import scala.io.Source
import scala.util.matching.Regex
// import circe here from game of thrones

// schema: Schema[EthereumTransaction]
class FileSpoutExecutor[T](
    var source: String = "",
    schema: Schema[T],
    lineConverter: (String => T),
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends SpoutExecutor[T](conf: Config, pulsarController: PulsarController, scheduler) {

  private val topic    = conf.getString("raphtory.spout.topic")
  private val producer = pulsarController.createProducer(schema, topic)
  // set persistency of completes files topic
  setupNamespace()

  private val fileReadProducerTopic =
    "persistent://public/raphtory_spout/completedFiles_" + deploymentID

  private val fileTrackerProducer =
    pulsarController.createProducer(Schema.BYTES, fileReadProducerTopic)

  private val fileTrackerConsumer = pulsarController.accessClient
    .newConsumer(schema)
    .topics(Array("fileReadProducerTopic" + deploymentID).toList.asJava)
    .subscriptionName("fileReaderTracker")
    .subscriptionType(SubscriptionType.Exclusive)
    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    .batchReceivePolicy(
            BatchReceivePolicy.builder().maxNumMessages(10000).timeout(4, TimeUnit.SECONDS).build()
    )
    .poolMessages(true)
    .subscribe() // createConsumer("fileReaderTracker", fileReadProducerTopic)

  private val completedFiles: Set[String] = Set()
  private val reReadFiles                 = conf.getBoolean("raphtory.spout.file.local.reread")

  // regex is ignored if file is not a directory
  private val file_regex = new Regex(conf.getString("raphtory.spout.file.local.fileFilter"))
  private val recurse    = conf.getBoolean("raphtory.spout.file.local.recurse")

  //  private val hardLinkFile = conf.getBoolean("raphtory.spout.hardLinkCopy")
  // TODO HARDLINK wont work on a network share
  private val outputDirectory = conf.getString("raphtory.spout.file.local.outputDirectory")

  if (source == "")
    source = conf.getString("raphtory.spout.file.local.sourceDirectory")

  def setupNamespace(): Unit =
    try pulsarController.pulsarAdmin.namespaces().createNamespace("public/raphtory_spout")
    catch {
      case error: PulsarAdminException =>
        logger.warn("Namespace already found")
    } finally pulsarController.setRetentionNamespace("public/raphtory_spout")

  def updateFilesRead(): Unit = {
    // get names/path of all files that have been previously read, only prepopulate once
    if (reReadFiles) {
      logger.debug("Not adding previously read files to tracker.")
      return
    }
    logger.debug("Adding previously read files to tracker.")
    if (!fileTrackerConsumer.hasReachedEndOfTopic)
      fileTrackerConsumer.batchReceive().forEach(msg => completedFiles.add(new String(msg.getData)))
  }

  override def run(): Unit = {
    updateFilesRead()
    readFiles()
  }

  class SpoutScheduler extends Runnable {

    def run() {
      readFiles()
    }
  }

  override def stop(): Unit =
    producer.close()

  def getListOfFiles(dir: File, regex: Regex, recurse: Boolean): List[File] =
    if (dir.isDirectory) {
      var found = dir.listFiles.filter(_.isFile).toList.filter(file => regex.findFirstIn(file.getName).isDefined)
      if (recurse)
        found =
          found ++ dir.listFiles.filter(_.isDirectory).flatMap(getListOfFiles(_, regex, recurse))
      found
    } else
      List(dir)

  def readFiles(): Unit = {
    // check if exists
    if (!Files.exists(Paths.get(source)))
      throw new FileNotFoundException(s"File '$source' does not exist.")
    logger.debug(s"Found file '$source'.")

    if (!Files.isReadable(Paths.get(source))) {
      logger.warn(f"ERROR: $source%s is not readable")
      return
    }

    logger.debug(f"$source%s is readable")
    // if so then check recurse, filter
    val files = getListOfFiles(new File(source), file_regex, recurse)
    if (files.isEmpty) return
    // create temp folder if it doesnt exist
    val tempOutputDirectory = new File(outputDirectory)
    logger.debug(s"Creating temp folder '$tempOutputDirectory'.")
    if (!tempOutputDirectory.exists) {
      if (!tempOutputDirectory.mkdirs()) {
        logger.warn(f"ERROR: COULD NOT MAKE DIR  $tempOutputDirectory%s")
        return
      }
    } else
      // if the folder does exist then delete the files inside of it
      tempOutputDirectory.listFiles().foreach(f => f.delete())
    // then hard link each file and copy them to the new folder
    // println("Hard linking...")
    try files.foreach { fileToCopy =>
      // do not hardlink files we already read
      if (!completedFiles.contains(fileToCopy.getPath)) {
        logger.debug(f"Hard linking $fileToCopy%s")
        Files.createLink(
                Paths.get(tempOutputDirectory.getPath + "/" + fileToCopy.getName),
                fileToCopy.toPath
        )
      }
    } catch {
      case error: UnsupportedOperationException =>
        logger.error(
                "UnsupportedOperationException: System does not support adding an existing file to a directory."
        )

        return
      case error: FileAlreadyExistsException =>
        logger.warn(
                "FileAlreadyExistsException: Could not create hardlink, file already exists."
        )

        return
      case error: IOException => logger.warn("ERROR(IOException): an I/O error occurred "); return;
      case error: SecurityException =>
        logger.warn("ERROR(SecurityException): Invalid permissions to create hardlink"); return;
    }

    tempOutputDirectory.listFiles.sorted.foreach { f =>
      logger.info(f"Reading $f%s")
      var source = Source.fromFile(f)
      if (f.getPath.toLowerCase.endsWith(".gz"))
        source = Source.fromInputStream(new GZIPInputStream(new FileInputStream(f.getPath)))
      else if (f.getPath.toLowerCase.endsWith(".zip"))
        source = Source.fromInputStream(new ZipInputStream(new FileInputStream(f.getPath)))
      val fileSize: Long     = f.length
      val percentage: Double = 100.0 / fileSize
      var readLength         = 0
      var progressPercent    = 1
      var divisByTens        = 10
      for (line <- source.getLines()) {
        readLength += line.length
        progressPercent = Math.ceil(percentage * readLength).toInt
        val test = lineConverter(line)
        producer.newMessage().value(test).sendAsync()
        //        if (progressPercent % divisByTens == 0) {
        //          divisByTens += 10
        //          logger.info(f"Read: $progressPercent%d%% of file.")
        //        }
      }
      source.close()
      logger.debug("Finished reading file.")
      // then remove the hard link
      try Files.delete(f.toPath)
      catch {
        case _: NoSuchFileException =>
          logger.warn("ERROR(NoSuchFileException): File does not exist");
        case _: DirectoryNotEmptyException =>
          logger.warn("ERROR(DirectoryNotEmptyException) Cannot delete directory, not empty");
        case _: IOException       => logger.warn("ERROR(IOException): an IO Error occurred");
        case _: SecurityException => logger.warn("ERROR(SecurityException) Security error");
      }
    }
    // add file to tracker so we do not read it again
    if (!reReadFiles) {
      logger.debug("Adding file(s) to completed list.")
      files.foreach { f =>
        if (!completedFiles.contains(f.getPath)) {
          completedFiles.add(f.getPath)
          fileTrackerProducer.sendAsync(f.getPath.getBytes)
        }
      }
    }

    logger.debug("Finished reading all files.")
    scheduler.scheduleOnce(10, TimeUnit.SECONDS, new SpoutScheduler())
  }

}
