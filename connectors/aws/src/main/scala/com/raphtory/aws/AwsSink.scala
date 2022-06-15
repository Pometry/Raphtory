package com.raphtory.aws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{AbortMultipartUploadRequest, CompleteMultipartUploadRequest, InitiateMultipartUploadRequest, ObjectMetadata, PartETag, PutObjectRequest, UploadPartRequest}
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.{FormatAgnosticSink, SinkConnector}
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config
import org.apache.commons.io.output.ByteArrayOutputStream
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

case class AwsSink(awsS3OutputFormatBucketName: String, awsS3OutputFormatBucketKey: String, format: Format = CsvFormat())
  extends FormatAgnosticSink(format) {

  override protected def buildConnector(
                                         jobID: String,
                                         partitionID: Int,
                                         config: Config,
                                         itemDelimiter: String
                                       ): SinkConnector =
    new SinkConnector {

      private val credentials: AwsCredentials = AwsS3Connector().getAWSCredentials()

      private val s3Client: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withCredentials(
          new AWSStaticCredentialsProvider(credentials)
        )
        .build()

      private val request = s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(
        awsS3OutputFormatBucketName,
        awsS3OutputFormatBucketKey
        )
      )

      private val etags = ListBuffer[PartETag]()
      private var partNumber = 1
      private var partSize = 5 * 1024 * 1024
      private var uploadRequest: Option[UploadPartRequest] = None
      private var stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      private var stream0: ByteArrayOutputStream = new ByteArrayOutputStream()
      private var stream1: ByteArrayOutputStream = new ByteArrayOutputStream()
      private var stream2: ByteArrayOutputStream = new ByteArrayOutputStream()
      private var stream3: ByteArrayOutputStream = new ByteArrayOutputStream()

      override def write(value: String): Unit = {
         partitionID match {
            case 0 =>  stream0.write(value.getBytes(StandardCharsets.UTF_8))
            case 1 =>  stream1.write(value.getBytes(StandardCharsets.UTF_8))
            case 2 =>  stream2.write(value.getBytes(StandardCharsets.UTF_8))
            case 3 =>  stream3.write(value.getBytes(StandardCharsets.UTF_8))
          }
      }

      /** Closes the current item
       *
       * @note Intended to be used in combination with append.
       */

        override def closeItem(): Unit = {

          if (stream0.size() + stream1.size() + stream2.size() + stream3.size() >= partSize) {

            stream = partitionID match {
              case 0 => stream0
              case 1 => stream1
              case 2 => stream2
              case 3 => stream3
            }

            uploadRequest = Some(new UploadPartRequest()
              .withBucketName(request.getBucketName)
              .withKey(request.getKey)
              .withUploadId(request.getUploadId)
              .withPartNumber(partNumber)
              .withPartSize(partSize)
              .withInputStream(stream.toInputStream))

            partNumber += 1
            etags += s3Client.uploadPart(uploadRequest.get).getPartETag
          }
        }


      override def close(): Unit = {
        uploadRequest match {
          case Some(value) => s3Client.completeMultipartUpload(
            new CompleteMultipartUploadRequest(
              awsS3OutputFormatBucketName,
              awsS3OutputFormatBucketKey,
              value.getUploadId,
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

          case None =>

            var metadata = new ObjectMetadata()

            partitionID match {
              case 0 =>
                metadata.setContentLength(stream0.size())
                s3Client.putObject(new PutObjectRequest(awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID", stream0.toInputStream, metadata))

              case 1 =>
                metadata.setContentLength(stream1.size())
                s3Client.putObject(new PutObjectRequest(awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID", stream1.toInputStream, metadata))

              case 2 =>
                metadata.setContentLength(stream2.size())
                s3Client.putObject(new PutObjectRequest(awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID", stream2.toInputStream, metadata))

              case 3 =>
                metadata.setContentLength(stream3.size())
                s3Client.putObject(new PutObjectRequest(awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID", stream3.toInputStream, metadata))

            }
          }
        }
     }
  }
