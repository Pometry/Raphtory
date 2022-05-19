package com.raphtory.config

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.S3CredentialsProviderChain
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.ClientConfiguration
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object

import java.io.BufferedReader
import java.io.InputStreamReader
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import org.apache.http.client.CredentialsProvider

import java.io.File

object AWSUpload extends App {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  val bucketName: String     = raphtoryConfig.getString("raphtory.aws.bucketName")
  val filePath: String       = raphtoryConfig.getString("raphtory.aws.inputFilePath")
  val uploadFileName: String = raphtoryConfig.getString("raphtory.aws.uploadFileName")
  //file to upload
  val fileToUpload           = new File(filePath)

  // This will create a bucket for storage
  //  amazonS3Client.createBucket(bucketName)
//  AwsS3Client.amazonS3Client.putObject(bucketName, uploadFileName, fileToUpload)

  // Tests successful upload of the file by reading first line
//  val s3object: S3Object =
//    AwsS3Client.amazonS3Client.getObject(new GetObjectRequest(bucketName, uploadFileName))
//  val in                 = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
//
//  val line = in.readLine
//  val data = s"$line"
//  println("Printing first line of fetched file: ")
//  println(data)

}

object AwsS3Client {}
