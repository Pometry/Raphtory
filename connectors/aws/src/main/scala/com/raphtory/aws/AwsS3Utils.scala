package com.raphtory.aws

import com.amazonaws.auth.{AWSCredentials, AWSSessionCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest

/**
 * @param sts
 * AWSAssumeRole uses an AWS Security Token Service object created in
 * object AWSStsClient(region: Regions)
 * and uses this to to get temporary credentials from AWS:
 *
 * def getTempCredentials(
 *                         arnToken: String,
 *                         durationSeconds: Int,
 *                         tokenCode: String
 *                       ): AwsCredentials
 *
 * This method requires an Amazon Resource Number token `arnToken: String`,
 * an expiration time in seconds for the MFA token `durationSeconds: Int`,
 * and an multi-factor authentication code to access AWS `tokenCode: String`.
 */

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

/**
 * @param region
 * AWSStsClient(region: Regions) builds an AWSSecurityTokenService object
 * so that temporary AWS credentials can be created. It requires a region
 * to be specified (e.g. `Regions.US_WEST_1`, `Regions.US_EAST_1`).
 */

class AWSStsClient(region: Regions) {
  val stsClient: AWSSecurityTokenService =
    AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build()
}

object AWSStsClient {
  def apply(region: Regions) = new AWSStsClient(region)
}

/**
 *
 * @param accessKeyId
 * @param secretAccessKey
 *
 * `AwsCredentials` defines the credentials needed to build an S3Client. It requires `accessKeyId` and `secretAccessKey`,
 * and if it is an AwsSessionCredential, it also requires `token`. These methods allow you to retrieve these keys and tokens.
 */

class AwsCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials {
  override def getAWSAccessKeyId: String = accessKeyId

  override def getAWSSecretKey: String = secretAccessKey
}

/**
 *
 * @param accessKeyId
 * @param secretAccessKey
 *
 * `AwsCredentials` defines the credentials needed to build an S3Client. It requires `accessKeyId` and `secretAccessKey`,
 * and if it is an AwsSessionCredential, it also requires `token`. These methods allow you to retrieve these keys and tokens.
 * @param token
 */

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

