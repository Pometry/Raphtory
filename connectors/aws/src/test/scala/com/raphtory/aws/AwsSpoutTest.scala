package com.raphtory.aws

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList

/**
  * Tests the AWS S3 Spout and Sink, requires bucket name and bucket path that you would like to ingest.
  * Also requires bucket to output results into. Both set in application.conf.
  */

object AwsSpoutTest extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val config                        = Raphtory.getDefaultConfig()
    val awsS3SpoutBucketName          = config.getString("raphtory.spout.aws.local.spoutBucketName")
    val awsS3SpoutBucketKey          = config.getString("raphtory.spout.aws.local.spoutBucketPath")
    val awsS3OutputFormatBucketName = config.getString("raphtory.spout.aws.local.outputBucketName")

    val source               = AwsS3Spout(awsS3SpoutBucketName,awsS3SpoutBucketKey)
    val builder              = new LotrGraphBuilder()
    val output               = AwsS3Sink(awsS3OutputFormatBucketName)
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      IO {
        graph
          .at(32674)
          .past()
          .execute(EdgeList())
          .writeTo(output)
          .waitForJob()
        ExitCode.Success
      }
    }
  }
}
