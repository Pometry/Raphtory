package com.raphtory.examples.gabMining.utils
import java.io.{BufferedWriter, FileWriter}

// this class is used to write to a file all the results obtained from the Live Analysis.
// The file is written line by line.
class writeToFile {

  def writeLines(fileName: String , line: String) : Unit = {
    val file=new FileWriter(fileName, true)
    var bw =new BufferedWriter(file)
    bw.write(line)
    bw.newLine()
    bw.flush()
  }
}
