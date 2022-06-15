package com.raphtory.aws

import com.raphtory.Raphtory

case class AwsS3Connector() {

  val raphtoryConfig               = Raphtory.getDefaultConfig()
  private val accessKey       = raphtoryConfig.getString("raphtory.spout.aws.local.accessKey")
  private val secretAccessKey = raphtoryConfig.getString("raphtory.spout.aws.local.secretAccessKey")
  private val sessionToken       = raphtoryConfig.getString("raphtory.spout.aws.local.sessionToken")

  private val credentials: AwsCredentials =
    AwsCredentials(accessKey, secretAccessKey, sessionToken)

  def getAWSCredentials(): AwsCredentials = credentials
}
