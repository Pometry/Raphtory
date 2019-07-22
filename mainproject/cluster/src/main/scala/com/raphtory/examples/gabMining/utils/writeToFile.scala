package com.raphtory.examples.gabMining.utils
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}


class writeToFile {

  def writeLines(fileName: String , line: String) : Unit = {
    val name=s"/Users/lagordamotoneta/Documents/QMUL/QMUL/project/Datasets/$fileName"
    val file=new FileWriter(name, true)
    var bw =new BufferedWriter(file)
    bw.write(line)
    bw.newLine()
    bw.flush()
  }
}
