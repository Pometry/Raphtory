package com.raphtory.config

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.securitytoken.AWSSecurityTokenService
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest

class AWSAssumeRole(sts: AWSSecurityTokenService) {

  def getTempCredentials(
      roleARN: String,
      durationSeconds: Int,
      tokenCode: String
  ): AwsCredentials = {

    val assumeRole = sts
      .assumeRole(
              new AssumeRoleRequest()
                .withRoleArn(roleARN)
                .withDurationSeconds(durationSeconds)
                .withTokenCode(tokenCode)
      )
      .getCredentials

    AwsCredentials(
            assumeRole.getAccessKeyId,
            assumeRole.getSecretAccessKey,
            assumeRole.getSessionToken
    )

  }
}

object AWSAssumeRole {
  def apply(sts: AWSSecurityTokenService) = new AWSAssumeRole(sts)
}

/**
  * @param accessKeyId = ACCESS_KEY_ENV_VAR
  * @param secretAccessKey = SECRET_KEY_ENV_VAR
  */

class AwsCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials {
  override def getAWSAccessKeyId: String = accessKeyId

  override def getAWSSecretKey: String = secretAccessKey
}

/**
  * @param accessKeyId = ACCESS_KEY_ENV_VAR
  * @param secretAccessKey = SECRET_KEY_ENV_VAR
  * @param token = AWS_SESSION_TOKEN_ENV_VAR
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

/**
  * Methods to get credentials from environment variables
  */

trait CredentialsProvider extends AWSCredentialsProvider {
  override def getCredentials: AwsCredentials
  override def refresh(): Unit
}

class DefaultCredentialsProvider extends CredentialsProvider {
  private val provider = new EnvironmentVariableCredentialsProvider

  override def getCredentials: AwsCredentials =
    provider.getCredentials match {
      case sc: AWSSessionCredentials =>
        AwsCredentials(sc.getAWSAccessKeyId, sc.getAWSSecretKey, sc.getSessionToken)
      case c                         => AwsCredentials(c.getAWSAccessKeyId, c.getAWSSecretKey)
    }

  override def refresh(): Unit = provider.refresh()
}

object DefaultCredentialsProvider {

  def apply(): DefaultCredentialsProvider =
    new DefaultCredentialsProvider
}
