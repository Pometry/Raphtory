package com.raphtory.aws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.raphtory.api.input.Spout
import java.io.BufferedReader
import java.io.InputStreamReader

/**
  * @param awsS3SpoutBucketName
  * @param awsS3SpoutBucketPath
  *
  * The AwsS3Spout takes in the name and path of the AWS S3 bucket that you would like
  * to ingest into Raphtory, usually this is set in your tests.
  *
  * It builds an S3 client using credentials obtained through providing access keys.
  * The data is streamed from AWS S3 until null is reached.
  */

class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String) extends Spout[String] {

  val credentials: AwsCredentials = AwsS3Connector().getAWSCredentials()

  private val s3Client: AmazonS3 = AmazonS3ClientBuilder
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
    if (s3Reader.readLine() != null)
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
