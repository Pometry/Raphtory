package com.raphtory.examples.gabMining.utils
import java.io.{BufferedWriter, FileWriter}


class writeToFile {

  def writeLines(fileName: String , line: String) : Unit = {
    val file=new FileWriter(fileName, true)
    var bw =new BufferedWriter(file)
    bw.write(line)
    bw.newLine()
    bw.flush()
  }
}
