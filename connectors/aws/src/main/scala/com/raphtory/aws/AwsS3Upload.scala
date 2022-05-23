package com.raphtory.aws

import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import java.io.BufferedReader
import java.io.InputStreamReader
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import java.io.File

// TODO: Currently only uploading one line of data, will be implemented properly with AWS multi-part uploading.

object AWSS3Upload extends App {

    val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

    val raphtoryConfig: Config = ConfigFactory.load()

    val spoutBucketName: String = raphtoryConfig.getString("raphtory.spout.aws.local.spoutBucketName")
    val spoutBucketPath: String = raphtoryConfig.getString("raphtory.spout.aws.local.spoutBucketPath")
    val uploadBucketName: String     = raphtoryConfig.getString("raphtory.spout.aws.local.uploadBucketName")
    val inputFilePath: String       = raphtoryConfig.getString("raphtory.spout.aws.local.inputFilePath")
    val uploadFileName: String = raphtoryConfig.getString("raphtory.spout.aws.local.uploadFileName")
    val fileToUpload           = new File(inputFilePath)
    val AwsS3Client = AwsS3Spout(spoutBucketName, spoutBucketPath).s3Client

//Create AWS Bucket and upload file
    AwsS3Client.createBucket(uploadBucketName)
    AwsS3Client.putObject(uploadBucketName, uploadFileName, fileToUpload)

//   Tests successful upload of the file by reading first line
    val s3object: S3Object =
      AwsS3Client.getObject(new GetObjectRequest(uploadBucketName, uploadFileName))
    val in                 = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
    val line = in.readLine
    logger.info(s"Printing first line of uploaded file: $line")
}
