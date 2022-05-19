package com.raphtory.config

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.profile.internal.ProfileAssumeRoleCredentialsProvider
import com.amazonaws.auth.profile.internal.securitytoken.RoleInfo
import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.securitytoken
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest

class AwsS3Client {

  def load(): CredentialsProvider = {
    val provider = DefaultCredentialsProvider()
    if (tryCredentials(provider))
      provider
    else
      throw new IllegalStateException(
              s"Failed to load AWS Credentials. Check your environment or configuration."
      )
  }

  def tryCredentials(provider: CredentialsProvider): Boolean =
    try {
      provider.getCredentials
      true
    }
    catch {
      case e: AmazonClientException => false
    }

  def getS3Client(useStsRole: Boolean, stsRoleArn: String): AmazonS3 =
    if (useStsRole)
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(
                new STSProfileCredentialsServiceProvider(
                        new RoleInfo()
                          .withLongLivedCredentialsProvider(load())
                          .withRoleArn(stsRoleArn)
                )
        )
        .build()
    else
      AmazonS3ClientBuilder.standard().build()
}

object AwsS3Client {
  def apply() = new AwsS3Client()
}
