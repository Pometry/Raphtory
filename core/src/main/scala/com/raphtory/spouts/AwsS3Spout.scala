package com.raphtory.spouts

import com.amazonaws.auth.profile.internal.securitytoken.RoleInfo
import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.securitytoken.AWSSecurityTokenService
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import java.io.BufferedReader
import java.io.InputStreamReader
import com.raphtory.components.spout.Spout
import com.raphtory.config.AWSAssumeRole
import com.raphtory.config.AwsCredentials
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config

case class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String)
        extends Spout[String] {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  private val roleArn         = raphtoryConfig.getString("raphtory.aws.roleArn")
  private val durationSeconds = raphtoryConfig.getInt("raphtory.aws.durationSeconds")
  private val tokenCode       = raphtoryConfig.getString("raphtory.aws.tokenCode")

  val stsClient: AWSSecurityTokenService =
    AWSSecurityTokenServiceClientBuilder.standard().withRegion(Regions.US_WEST_2).build()

  val credentials: AwsCredentials =
    AWSAssumeRole(stsClient).getTempCredentials(roleArn, durationSeconds, tokenCode)

  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withCredentials(
            new STSProfileCredentialsServiceProvider(
                    new RoleInfo().withLongLivedCredentials(credentials)
            )
    )
    .build()

  val s3object: S3Object        =
    s3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, awsS3SpoutBucketPath))
  val s3Reader                  = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
  var line: Option[String]      = Option(s3Reader.readLine)
  override def hasNext: Boolean = line.isDefined

  override def next(): String = {
    val data = s3Reader.readLine
    println(data)
    data
  }

  override def close(): Unit =
    logger.debug(s"Spout for AWS '$awsS3SpoutBucketName' finished")

  override def spoutReschedules(): Boolean = false
}
