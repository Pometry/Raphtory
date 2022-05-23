package com.raphtory.aws

import com.amazonaws.auth.{AWSCredentials, AWSSessionCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest

class AWSAssumeRole(sts: AWSSecurityTokenService) {

  def getTempCredentials(
                          arnToken: String,
                          durationSeconds: Int,
                          tokenCode: String
                        ): AwsCredentials = {

    val credentials = sts.getSessionToken(new GetSessionTokenRequest()
      .withSerialNumber(arnToken)
      .withTokenCode(tokenCode)
      .withDurationSeconds(durationSeconds)).getCredentials


    AwsCredentials(
      credentials.getAccessKeyId,
      credentials.getSecretAccessKey,
      credentials.getSessionToken
    )

  }
}

object AWSAssumeRole {
  def apply(sts: AWSSecurityTokenService) = new AWSAssumeRole(sts)
}

class AWSStsClient(region: Regions) {
  val stsClient: AWSSecurityTokenService =
    AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build()
}

object AWSStsClient {
  def apply(region: Regions) = new AWSStsClient(region)
}

class AwsCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials {
  override def getAWSAccessKeyId: String = accessKeyId

  override def getAWSSecretKey: String = secretAccessKey
}

class AwsSessionCredentials(accessKeyId: String, secretAccessKey: String, token: String)
  extends AwsCredentials(accessKeyId, secretAccessKey)
    with AWSSessionCredentials {
  override def getSessionToken: String = token
}

object AwsCredentials {

  def apply(accessKeyId: String, secretAccessKey: String): AwsCredentials =
    new AwsCredentials(accessKeyId, secretAccessKey)

  def apply(accessKeyId: String, secretAccessKey: String, token: String): AwsCredentials =
    new AwsSessionCredentials(accessKeyId, secretAccessKey, token)
}

