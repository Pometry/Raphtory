package com.raphtory.fileSpoutTest

import com.raphtory.util.FileUtils
import com.raphtory.util.FileUtils.logger
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.UUID
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.util

class FileSpoutTest extends AnyFunSuite with BeforeAndAfterAll {

  val tempDir       = Files.createTempDirectory("FileSpoutTest").toFile.getAbsolutePath
  val ownerWritable = PosixFilePermissions.fromString("-w-------")
  val permissions   = PosixFilePermissions.asFileAttribute(ownerWritable)
  val tempFile      = Files.createTempFile(UUID.randomUUID().toString, ".hy", permissions)
  val tempDirs      = new util.ArrayList[String]()
  tempDirs.add(tempDir.toString)

  test("FileUtils.validatePath should return true for tempDir if it exists") {
    assert(FileUtils.validatePath(tempDir))
  }
  test("FileUtils.validatePath should throw if file does not exist") {
    val randomName = "/" + UUID.randomUUID().toString + ".hy"
    assertThrows[java.io.FileNotFoundException](FileUtils.validatePath(randomName))
  }
//  test("FileUtils.validatePath should throw if file cannot be read"){
//    // make a temp file
//    assertThrows[IllegalStateException](FileUtils.validatePath(tempFile.toAbsolutePath.toString))
//  }
  test("FileUtils.deletefile should delete a file") {
    val tempFile = Files.createTempFile(UUID.randomUUID().toString, ".hy")
    assert(FileUtils.deleteFile(tempFile.toAbsolutePath))
  }
  test("FileUtils.createOrCleanDirectory should be allowed to make new temp folders") {
    val newDir = tempDir + "/" + UUID.randomUUID().toString
    assert(FileUtils.createOrCleanDirectory(newDir).getAbsolutePath === newDir)
    tempDirs.add(newDir)
  }
//  test("FileUtils.createOrCleanDirectory should be allowed to clean existing temp folders"){
//    // make a new temp folder
//    val randomUUID = UUID.randomUUID().toString
//    val tempDirx = Files.createTempDirectory(randomUUID)
//    val tempFilex = Files.createFile(new File(tempDirx.toAbsolutePath.toString+"/"+randomUUID).toPath)
//    FileUtils.createOrCleanDirectory(tempDirx.toAbsolutePath.toString)
//    assert(new File(tempDirx.toAbsolutePath.toString).listFiles().isEmpty)
//    tempDirs.add(tempDirx.toAbsolutePath.toString)
//  }

  override def afterAll(): Unit =
    scala.util.control.Exception.ignoring(classOf[Exception]) {
      tempDirs.forEach(filex => Files.deleteIfExists(new File(filex).toPath))
      Files.deleteIfExists(tempFile)
    }

}
