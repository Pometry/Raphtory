package com.raphtory.spouts

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import java.io.BufferedReader
import java.io.InputStreamReader
import com.raphtory.components.spout.Spout
import com.raphtory.config.AwsS3Client

case class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String)
        extends Spout[String] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val s3 = AwsS3Client

  val s3object: S3Object        =
    s3.amazonS3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, awsS3SpoutBucketPath))
  val s3Reader                  = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
  var line: Option[String]      = Option(s3Reader.readLine)
  var lineNo                    = 1
  var count                     = 0
  val fileLines: Int            = s3.numberOfFileLines
  override def hasNext: Boolean = line.isDefined

  override def next(): String = {
    val data = s3Reader.readLine
    lineNo += 1
    count += 1
    if (count % fileLines == 0)
      while (data != null)
        logger.debug(s"AWS spout sent $count messages.")
    data
  }

  override def close(): Unit =
    logger.debug(s"Spout for AWS '$awsS3SpoutBucketName' finished, edge count: ${lineNo - 1}")

  override def spoutReschedules(): Boolean = false
}
