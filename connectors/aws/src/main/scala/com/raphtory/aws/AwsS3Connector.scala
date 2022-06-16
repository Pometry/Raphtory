package com.raphtory.aws

import com.raphtory.Raphtory

/*
To use AwsS3Connector(), you must provide an access key, secret access key and session token in application.conf.
This can be obtained by running this command in your terminal:

``````
aws sts get-session-token --serial-number your-AWS-mfa-resource-token --token-code your-mfa-code --duration-seconds 3600
``````

which should return:
{
    "Credentials": {
        "AccessKeyId": "",
        "SecretAccessKey": "",
        "SessionToken": "",
        "Expiration": "2022-06-15T12:38:11+00:00"
    }
}

 */

case class AwsS3Connector() {

  val raphtoryConfig               = Raphtory.getDefaultConfig()
  private val accessKey       = raphtoryConfig.getString("raphtory.spout.aws.local.accessKey")
  private val secretAccessKey = raphtoryConfig.getString("raphtory.spout.aws.local.secretAccessKey")
  private val sessionToken       = raphtoryConfig.getString("raphtory.spout.aws.local.sessionToken")

  private val credentials: AwsCredentials =
    AwsCredentials(accessKey, secretAccessKey, sessionToken)

  def getAWSCredentials(): AwsCredentials = credentials
}
