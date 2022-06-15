package com.raphtory.aws

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList

/**
  * Test to use the AWS S3 Spout, requires bucket name and bucket path that you would like to ingest,
  * set in application.conf.
  */

object AwsSpoutTest extends App {
  val config               = Raphtory.getDefaultConfig()
  val awsS3SpoutBucketName = config.getString("raphtory.spout.aws.local.spoutBucketName")
  val awsS3SpoutBucketPath = config.getString("raphtory.spout.aws.local.spoutBucketPath")
  val inputFilePath = config.getString("raphtory.spout.aws.local.inputFilePath")
  val awsS3OutputFormatBucketName = config.getString("raphtory.spout.aws.local.outputBucketName")
  val awsS3OutputFormatBucketKey = config.getString("raphtory.spout.aws.local.outputBucketKey")

  val source = AwsS3Spout(awsS3SpoutBucketName,awsS3SpoutBucketPath)
  val builder              = new LOTRGraphBuilder()
  val graph                = Raphtory.stream(spout = source, graphBuilder = builder)
  val output               = AwsSink(awsS3OutputFormatBucketName, awsS3OutputFormatBucketKey)


  val queryHandler = graph
    .at(32674)
    .past()
    .execute(EdgeList())
    .writeTo(output)

  queryHandler.waitForJob()
}
