package com.raphtory.core.analysis.serialisers
import com.amazonaws.auth.BasicAWSCredentials
import java.io.{BufferedWriter, File, FileWriter}
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.regions.Regions

abstract class S3Serialiser extends Serialiser {
  val AWS_ACCESS_KEY = s"${sys.env.getOrElse("AWS_ACCESS_KEY", "")}"
  val AWS_SECRET_KEY = s"${sys.env.getOrElse("AWS_SECRET_KEY", "")}"
  val bucketName = s"${sys.env.getOrElse("AWS_BUCKET_NAME", "")}"
  val yourAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
  //val amazonS3Client = new AmazonS3Client(yourAWSCredentials)


  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(yourAWSCredentials)).build
  //s3Client.setRegion(com.amazonaws.regions.Region.getRegion(Regions.EU_WEST_2))
  try{s3Client.createBucket(bucketName)}catch {case e:Exception =>}

  override def write(serialisedResults:(Array[String],Array[String]),file:File) = {
    println("vertices & edges", serialisedResults._1.length, serialisedResults._2.length)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(startOfFile())
    bw.write(serialisedResults._1.mkString(rowDelimeter()))
    bw.write(middleOfFile())
    bw.write(serialisedResults._2.mkString(rowDelimeter()))
    bw.write(endOfFile())
    bw.newLine()
    bw.close()
    new Thread(() => {
        s3Client.putObject(bucketName, file.getName,file)
        file.delete()
    }).start()
  }
}
