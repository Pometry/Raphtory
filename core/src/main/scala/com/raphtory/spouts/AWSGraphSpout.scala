package com.raphtory.spouts

import com.raphtory.core.components.spout.BatchableSpout
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import java.io.{BufferedReader, InputStreamReader}
import com.amazonaws.services.s3.AmazonS3Client
import com.raphtory.core.deploy.AWSUpload
import com.raphtory.core.deploy.Raphtory
import com.typesafe.config.Config

case class AWSGraphSpout(bucketName: String, objectKey: String) extends BatchableSpout[String] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  
  val s3 = AWSUpload.getAWSClient()
  val s3object: S3Object = s3.getObject(new GetObjectRequest(bucketName, objectKey))
  val in = new BufferedReader(new InputStreamReader(s3object.getObjectContent()))
  var line: String = null
  var lineNo = 1
  var count  = 0

  override def hasNext: Boolean = (line = in.readLine) != null

  override def next(): String = {
    val line = in.readLine
    val data = s"$line"
    lineNo += 1
    count += 1
    if (count % 100_000 == 0)
      logger.debug(s"AWS spout sent $count messages.")
    data
  }

  override def close(): Unit = {
    logger.debug(s"Spout for AWS '$bucketName' finished, edge count: ${lineNo - 1}")
  }

  override def spoutReschedules(): Boolean = false
}
