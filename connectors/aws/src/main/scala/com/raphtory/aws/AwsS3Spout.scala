package com.raphtory.aws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.securitytoken.AWSSecurityTokenService
import com.raphtory.api.input.Spout

import java.io.BufferedReader
import java.io.InputStreamReader
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
  * @param awsS3SpoutBucketName
  * @param awsS3SpoutBucketPath
  *
  * The AwsS3Spout takes in the name and path of the AWS S3 bucket that you would like
  * to ingest into Raphtory, usually this is set in your tests.
  * It requires the `arnToken`, `durationSeconds` and `tokenCode` to be set in `application.conf`.
  *
  * It builds an S3 client using credentials obtained through multi-factor authentication.
  * The data is streamed from AWS S3 until null is reached.
  */

class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String) extends Spout[String] {

  val raphtoryConfig: Config = ConfigFactory.load()

  private val arnToken        = raphtoryConfig.getString("raphtory.spout.aws.local.amazonResourceName")
  private val durationSeconds = raphtoryConfig.getInt("raphtory.spout.aws.local.durationSeconds")
  private val tokenCode       = raphtoryConfig.getString("raphtory.spout.aws.local.mfaTokenCode")
  private val region          = Regions.US_EAST_1

  val stsClient: AWSSecurityTokenService = AWSStsClient(region).stsClient

  val credentials: AwsCredentials =
    AWSAssumeRole(stsClient).getTempCredentials(arnToken, durationSeconds, tokenCode)

  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withCredentials(
            new AWSStaticCredentialsProvider(credentials)
    )
    .build()

  val s3object: S3Object =
    s3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, awsS3SpoutBucketPath))
  val s3Reader           = new BufferedReader(new InputStreamReader(s3object.getObjectContent))

  override def hasNext: Boolean =
    s3Reader.readLine() != null

  override def next(): String =
    if ((s3Reader.readLine()) != null)
      s3Reader.readLine()
    else {
      logger.error(s"NullPointerException: check your data source")
      throw new NullPointerException
    }

  override def close(): Unit =
    logger.debug(s"Spout for AWS '$awsS3SpoutBucketName' finished")

  override def spoutReschedules(): Boolean = false
}

object AwsS3Spout {

  def apply(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String) =
    new AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketPath)
}
