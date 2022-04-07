package com.raphtory.core.deploy

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.{AmazonWebServiceClient, ClientConfiguration, SDKGlobalConfiguration}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata, PutObjectRequest, Region, S3Object}
import java.io.{BufferedReader, ByteArrayInputStream, File}
import com.raphtory.core.deploy.Raphtory
import com.typesafe.config.Config
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import java.nio.charset.StandardCharsets

object AWSUpload extends App {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  val bucketName: String        = raphtoryConfig.getString("raphtory.aws.bucket_name")
  val fileToUpload: String      = raphtoryConfig.getString("raphtory.aws.input_file_path")
  val uploadFileName: String    = raphtoryConfig.getString("raphtory.aws.upload_file_name")
  val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")

  def getAWSClient(): AmazonS3Client = {
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    //file to upload
    val fileToUpload = new File(fileToUpload)

    /* These Keys available  in “Security Credentials” of Amazon S3 account */
    val AWS_ACCESS_KEY = raphtoryConfig.getString("raphtory.aws.access_key")
    val AWS_SECRET_KEY = raphtoryConfig.getString("raphtory.aws.secret_access_key")
    val AWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val amazonS3Client = new AmazonS3Client(AWSCredentials, clientConfiguration)
    amazonS3Client
  }

  // This will create a bucket for storage
  val amazonS3Client = getAWSClient()
  amazonS3Client.createBucket(bucketName)

  amazonS3Client.putObject(bucketName, uploadFileName, fileToUpload)
  
  
  // Tests succesful upload of the file by reading first line
  val s3object: S3Object = amazonS3Client.getObject(new GetObjectRequest(bucketName, uploadFileName))
  val in = new BufferedReader(new InputStreamReader(s3object.getObjectContent()))

  val line = in.readLine
  val data = s"$line"
  println("Printing first line of fetched file: ")
  println(data)
  
}
