package com.raphtory.aws

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

/**
  * Tests the AWS S3 Spout and Sink, requires bucket name and bucket path that you would like to ingest.
  * Also requires bucket to output results into. Both set in application.conf.
  */

object AwsSpoutTest extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val config                      = ConfigBuilder().getDefaultConfig
      val awsS3SpoutBucketName        = config.getString("raphtory.spout.aws.local.spoutBucketName")
      val awsS3SpoutBucketKey         = config.getString("raphtory.spout.aws.local.spoutBucketPath")
      val awsS3OutputFormatBucketName = config.getString("raphtory.spout.aws.local.outputBucketName")

      val spout  = AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketKey)
      val output = AwsS3Sink(awsS3OutputFormatBucketName)
      val source = CSVEdgeListSource(spout)

      graph.load(source)

      graph
        .at(32674)
        .past()
        .execute(EdgeList())
        .writeTo(output)
        .waitForJob()
    }
}
