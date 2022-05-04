package com.raphtory.output

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.PutObjectResult
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.config.AWSUpload
import com.raphtory.deployment.Raphtory
import com.raphtory.time.Interval
import com.typesafe.config.Config
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

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

  val awsClient              = AWSUpload.getAWSClient()
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {
    val value  = window match {
      case Some(w) => s"$timestamp,$w,${row.getValues().mkString(",")}"
      case None    => s"$timestamp,${row.getValues().mkString(",")}"
    }
    val stream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8))
    awsClient.putObject(
            new PutObjectRequest(
                    awsS3OutputFormatBucketName,
                    awsS3OutputFormatBucketPath,
                    stream,
                    new ObjectMetadata
            )
    )
  }
}

object AwsS3OutputFormat {

  def apply(awsS3OutputFormatBucketName: String, awsS3OutputFormatBucketPath: String) =
    new AwsS3OutputFormat(awsS3OutputFormatBucketName, awsS3OutputFormatBucketPath)
}
