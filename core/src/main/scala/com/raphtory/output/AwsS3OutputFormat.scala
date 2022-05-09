package com.raphtory.output

import com.amazonaws.services.s3.internal.InputSubstream
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.ListPartsRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PartETag
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.model.UploadPartRequest
import com.amazonaws.services.s3.transfer.internal.CompleteMultipartUpload
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.config.AWSUpload
import com.raphtory.config.AwsS3Client
import com.raphtory.time.Interval

import java.io.ByteArrayInputStream
import java.lang.Exception
import java.nio.charset.StandardCharsets
import scala.util.Random

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
class AwsS3OutputFormat(awsS3OutputFormatBucketName: String, awsS3OutputFormatBucketPath: String)
        extends OutputFormat {

  val s3 = AwsS3Client

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {
    //    s3.amazonS3Client
    //      .putObject(
    //              awsS3OutputFormatBucketName,
    //              awsS3OutputFormatBucketPath,
    //        stream ,
    //              new ObjectMetadata
    //      )

    val request =
      new InitiateMultipartUploadRequest(awsS3OutputFormatBucketName, awsS3OutputFormatBucketPath)

    val result = s3.amazonS3Client.initiateMultipartUpload(request)
    println(result)

    val value             = window match {
      case Some(w) => s"$timestamp,$w,${row.getValues().mkString(",")}"
      case None    => s"$timestamp,${row.getValues().mkString(",")}"
    }
    val stream            = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8))
    var partNumber        = 1
    val uploadPartRequest = new UploadPartRequest()
      .withBucketName(result.getBucketName)
      .withKey(result.getKey)
      .withUploadId(result.getUploadId)
      .withInputStream(new InputSubstream() stream)
      .withPartNumber {
        partNumber += 1
        partNumber
      }
      .withPartSize(5000)

    s3.amazonS3Client.uploadPart(uploadPartRequest)

    s3.amazonS3Client.completeMultipartUpload(
            new CompleteMultipartUploadRequest()
              .withBucketName(result.getBucketName)
              .withKey(result.getKey)
              .withUploadId(result.getUploadId)
    )
    val requestPartsList = new ListPartsRequest(
            result.getBucketName,
            result.getKey,
            result.getUploadId
    )

    val tags = s3.amazonS3Client.listParts(requestPartsList)
    println(tags)
    //    val abortRequest =
    //      new AbortMultipartUploadRequest(result.getBucketName, result.getKey, result.getUploadId)
    //    s3.amazonS3Client.abortMultipartUpload(abortRequest)

//    println(
//            s3.amazonS3Client
//              .getObjectMetadata(awsS3OutputFormatBucketName, awsS3OutputFormatBucketPath)
//    )
  }
}

object AwsS3OutputFormat {

  def apply(awsS3OutputFormatBucketName: String, awsS3OutputFormatBucketPath: String) =
    new AwsS3OutputFormat(awsS3OutputFormatBucketName, awsS3OutputFormatBucketPath)
}
