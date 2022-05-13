package com.raphtory.output

import com.amazonaws.ClientConfiguration
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult
import com.amazonaws.services.s3.model.PartETag
import com.amazonaws.services.s3.model.UploadPartRequest
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config

import java.io.ByteArrayInputStream
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class AwsS3OutputStream(
    awsS3OutputFormatBucketName: String,
    awsS3OutputFormatBucketKey: String
) {
  private var uploadRequest: UploadPartRequest       = _
  private var request: InitiateMultipartUploadResult = _
  private var s3Client: AmazonS3Client               = _
  private var partNumber                             = 1
  private var etags: ListBuffer[PartETag]            = _
  private val partSize: Int                          = 5 * 1024 * 1024
  private var stream: Array[Byte]                    = _

  def setup(): Unit = {
    //TODO: will soon be scrapped in place of MFA
    val raphtoryConfig: Config = Raphtory.getDefaultConfig()

    val clientConfiguration = new ClientConfiguration()
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    val awsS3ClientAccessKey: String = raphtoryConfig.getString("raphtory.aws.accessKey")

    val awsS3ClientSecretAccessKey: String =
      raphtoryConfig.getString("raphtory.aws.secretAccessKey")

    val AWSCredentials = new BasicAWSCredentials(awsS3ClientAccessKey, awsS3ClientSecretAccessKey)

    s3Client = new AmazonS3Client(AWSCredentials, clientConfiguration)

    // Initiate Multi-part Upload to Amazon S3
    request = s3Client.initiateMultipartUpload(
            new InitiateMultipartUploadRequest(
                    awsS3OutputFormatBucketName,
                    awsS3OutputFormatBucketKey
            )
    )

    etags = new ListBuffer[PartETag]()
  }

  // Gets bytes from AwsS3OutputFormat (results from algo's in form of bytes)
  def streamToAwsS3Client(bytesFromRaphtory: Array[Byte]) =
    stream = bytesFromRaphtory

  //Upload individual parts of 5mb size
  def uploadParts() = {
    uploadRequest = new UploadPartRequest()
      .withBucketName(request.getBucketName)
      .withKey(request.getKey)
      .withUploadId(request.getUploadId)
      .withPartNumber(partNumber)
      //.withFileOffset(0) possibly set to length of all lines
      .withPartSize(partSize)
      .withInputStream(new ByteArrayInputStream(stream, 0, stream.length))

    partNumber += 1
    etags += s3Client.uploadPart(uploadRequest).getPartETag
  }

  //Once all results have been outputted and uploaded into parts, the multi-part upload completes and is aborted
  def completeAndAbort() =
    if (etags.nonEmpty) {
      s3Client.completeMultipartUpload(
              new CompleteMultipartUploadRequest(
                      awsS3OutputFormatBucketName,
                      awsS3OutputFormatBucketKey,
                      uploadRequest.getUploadId,
                      etags.result().asJava
              )
      )
      s3Client.abortMultipartUpload(
              new AbortMultipartUploadRequest(
                      request.getBucketName,
                      request.getKey,
                      request.getUploadId
              )
      )
    }

}

object AwsS3OutputStream {

  def apply(
      awsS3OutputFormatBucketName: String,
      awsS3OutputFormatBucketKey: String
  ) = new AwsS3OutputStream(awsS3OutputFormatBucketName, awsS3OutputFormatBucketKey)
}
