package com.raphtory.output

import com.amazonaws.ClientConfiguration
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.event.ProgressEvent
import com.amazonaws.event.ProgressEventType
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest
import com.amazonaws.services.s3.model.ListPartsRequest
import com.amazonaws.services.s3.model.PartETag
import com.amazonaws.services.s3.model.UploadPartRequest
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.config.AwsS3Client
import com.raphtory.deployment.Raphtory
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.time.Interval
import com.typesafe.config.Config
import okhttp3.RequestBody
import com.amazonaws.services.s3.S3ResponseMetadata
import java.io.ByteArrayInputStream
import java.io.File
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

/**
  * {s}`AWSOutputFormat(awsBucketKey: String)`
  *   : writes output to an AWS bucket
  *
  *     {s}`awsBucketKey: String`
  *       : Bucket path to write to.
  *
  * Usage while querying or running algorithmic tests:
  *
  * ```{code-block} scala
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.AWSOutputFormat
  * import com.raphtory.core.algorithm.OutputFormat
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  * import com.raphtory.core.components.spout.Spout
  *
  * val graph = Raphtory.createGraph[T](Spout[T], GraphBuilder[T])
  * val outputFormat: OutputFormat = AWSOutputFormat("EdgeList")
  *
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.core.algorithm.OutputFormat),
  *  [](com.raphtory.core.client.RaphtoryClient),
  *  [](com.raphtory.core.client.RaphtoryGraph),
  *  [](com.raphtory.core.deploy.Raphtory)
  *  ```
  */
class AwsS3OutputFormat(
    awsS3OutputFormatBucketName: String,
    awsS3OutputFormatBucketKey: String
) extends OutputFormat {

  private var lineBytes: Array[Byte] = _

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit =
    lineBytes = (window match {
      case Some(w) => s"$timestamp,$w,${row.getValues().mkString(",")}"
      case None    => s"$timestamp,${row.getValues().mkString(",")}"
    }).getBytes(StandardCharsets.UTF_8)

  AwsS3OutputStream(awsS3OutputFormatBucketName, awsS3OutputFormatBucketKey).streamToAwsS3Client(
          lineBytes
  )

}

object AwsS3OutputFormat {

  def apply(
      awsS3OutputFormatBucketName: String,
      awsS3OutputFormatBucketKey: String
  ) =
    new AwsS3OutputFormat(awsS3OutputFormatBucketName, awsS3OutputFormatBucketKey)
}
