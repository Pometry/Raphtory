package com.raphtory.spouts

import com.amazonaws.ClientConfiguration
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object

import java.io.BufferedReader
import java.io.InputStreamReader
import com.raphtory.components.spout.Spout
import com.raphtory.config.AwsS3Client
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config

case class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String)
        extends Spout[String] {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")
  System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

  val awsS3ClientAccessKey: String = raphtoryConfig.getString("raphtory.aws.accessKey")

  val awsS3ClientSecretAccessKey: String =
    raphtoryConfig.getString("raphtory.aws.secretAccessKey")

  val AWSCredentials = new BasicAWSCredentials(awsS3ClientAccessKey, awsS3ClientSecretAccessKey)

  val s3Client = new AmazonS3Client(AWSCredentials, clientConfiguration)

  val s3object: S3Object        =
    s3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, awsS3SpoutBucketPath))
  val s3Reader                  = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
  var line: Option[String]      = Option(s3Reader.readLine)
  override def hasNext: Boolean = line.isDefined

  override def next(): String = {
    val data = s3Reader.readLine
    println(data)
    data
  }

  override def close(): Unit =
    logger.debug(s"Spout for AWS '$awsS3SpoutBucketName' finished")

  override def spoutReschedules(): Boolean = false
}
