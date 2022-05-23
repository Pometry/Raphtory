package com.raphtory.aws

import com.raphtory.deployment.Raphtory
import com.typesafe.config.{Config, ConfigFactory}


object AwsSpoutTest extends App {
  val config = ConfigFactory.load()
  val awsS3SpoutBucketName = config.getString("aws.local.spoutBucketName")
  val awsS3SpoutBucketPath = config.getString("aws.local.spoutBucketPath")
  val source = AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketPath)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
}
