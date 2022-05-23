package com.raphtory.aws

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.typesafe.config.ConfigFactory

/**
 * Test to use the AWS S3 Spout, requires bucket name and bucket path that you would like to ingest,
 * set in application.conf.
 */

object AwsSpoutTest extends App {
  val config = ConfigFactory.load()
  val awsS3SpoutBucketName = config.getString("raphtory.spout.aws.local.spoutBucketName")
  val awsS3SpoutBucketPath = config.getString("raphtory.spout.aws.local.spoutBucketPath")
  val source = AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketPath)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
  val output  = FileOutputFormat("/tmp/raphtory")

  val queryHandler = graph
    .at(32674)
    .past()
    .execute(EdgeList())
    .writeTo(output)

  queryHandler.waitForJob()
}
