package com.raphtory.aws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{AbortMultipartUploadRequest, CompleteMultipartUploadRequest, InitiateMultipartUploadRequest, ObjectMetadata, PartETag, PutObjectRequest, UploadPartRequest}
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.{FormatAgnosticSink, SinkConnector}
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.output.ByteArrayOutputStream
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/** A [[com.raphtory.api.output.sink.Sink Sink]] that writes a `Table` into files in AWS S3 using the given `format`.
 *
 * This sink creates a directory named after the jobID in your stated S3 output bucket. Each partition on the server then writes into its own file within this directory.
 *
 * @param awsS3OutputFormatBucketName the AWS S3 bucket name to write the table into
 * @param format the format to be used by this sink (`CsvFormat` by default)
 *
 * @example
 * {{{
 * import com.raphtory.algorithms.generic.EdgeList
 * import com.raphtory.aws.AwsS3Sink
 * import com.raphtory.aws.AwsS3Spout
 *
 * val graphBuilder = new YourGraphBuilder()
 * val graph = Raphtory.stream(AwsS3Spout("aws-bucket-name", "aws-bucket-key"), graphBuilder)
 * val bucketName = "bucket-name"
 * val sink = AWSS3Sink(bucketName)
 *
 * graph.execute(EdgeList()).writeTo(sink)
 * }}}
 * @see [[com.raphtory.api.output.sink.Sink Sink]]
 *      [[com.raphtory.api.output.format.Format Format]]
 *      [[com.raphtory.formats.CsvFormat CsvFormat]]
 *      [[com.raphtory.api.analysis.table.Table Table]]
 *      [[com.raphtory.Raphtory Raphtory]]
 */

case class AwsS3Sink(awsS3OutputFormatBucketName: String, format: Format = CsvFormat())
  extends FormatAgnosticSink(format) {

  override def buildConnector(jobID: String, partitionID: Int, config: Config, itemDelimiter: String, fileExtension: String): SinkConnector =

    new SinkConnector {

      /**
       * AWS Credentials needed to connect to AWS and build an S3 client.
       * Access Key, Secret Access Key and Session Token is needed in application.conf.
       */
      private val credentials: AwsCredentials = AwsS3Connector().getAWSCredentials()
      private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

      private val s3Client: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withCredentials(
          new AWSStaticCredentialsProvider(credentials)
        )
        .build()

      private val request = s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(
          awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID"
        )
      )

      private val etags = ListBuffer[PartETag]()
      private var partNumber = 1
      private var streamLimit = 5000000000L
      private var partSize: Long = 5 * 1024 * 1024
      private var uploadRequest: Option[UploadPartRequest] = None
      private var streamPosition: Long = 0L

      private var stream: ByteArrayOutputStream = new ByteArrayOutputStream()


      /*
      Write results to respective partition streams and add new line for every line of result
       */
      override def write(value: String): Unit = {
           stream.write((value + '\n').getBytes(StandardCharsets.UTF_8))
      }

      /**
       * If any of the streams of results are greater than 5GB, multipart upload to AWS will be used,
       * otherwise a regular one part upload will be used and will skip closeItem(),
       * going straight to close().
       */

      override def closeItem(): Unit = {

        if (stream.size() > streamLimit) {

          def makeUploadRequest(stream: ByteArrayOutputStream) = {
            // Upload file parts in 5MB parts
            while (streamPosition < stream.size()) {
              //To handle the last part which can be less than 5MB, adjusting part size as needed.
              partSize = Math.min(partSize, stream.size() - streamPosition)

              uploadRequest = Some(new UploadPartRequest()
                .withBucketName(request.getBucketName)
                .withKey(request.getKey)
                .withUploadId(request.getUploadId)
                .withPartNumber(partNumber)
                .withPartSize(partSize)
                .withFileOffset(streamPosition)
                .withInputStream(stream.toInputStream))

              etags += s3Client.uploadPart(uploadRequest.get).getPartETag
              partNumber += 1
              streamPosition += partSize
              logger.info(s"On part number: $partNumber, stream position in bytes: $streamPosition")
            }
          }
           makeUploadRequest(stream)


        }
      }

      /**
       * If multipart upload request has been found, close() will complete
       * the multipart upload and close the upload.
       *
       * If no upload request has been found, a single putObjectRequest will be made to upload
       * the results to AWS by partition.
       */

      override def close(): Unit = {
        uploadRequest match {
          case Some(value) =>
            s3Client.completeMultipartUpload(
              new CompleteMultipartUploadRequest(
                awsS3OutputFormatBucketName,
                s"$jobID/partition-$partitionID",
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

            def setContentLength(metadata: ObjectMetadata, stream: ByteArrayOutputStream): Unit = metadata.setContentLength(stream.size())

            def putObject(stream: ByteArrayOutputStream) = s3Client.putObject(new PutObjectRequest(awsS3OutputFormatBucketName, s"$jobID/partition-$partitionID", stream.toInputStream, metadata))

                setContentLength(metadata, stream)
                putObject(stream)

        }
      }
    }
}
