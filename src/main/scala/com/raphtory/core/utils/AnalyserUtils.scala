package com.raphtory.core.utils

import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser, LoadExternalAnalyser}

import scala.util.Try

object AnalyserUtils {
  def compileNewAnalyser(rawFile: String, args: Array[String]): Try[Analyser[Any]] =
    Try(LoadExternalAnalyser(rawFile, args).newAnalyser)

  def loadPredefinedAnalyser(className: String, args: Array[String]): Try[Analyser[Any]] =
    Try(Class.forName(className).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser[Any]])
      .orElse(Try(Class.forName(className).getConstructor().newInstance().asInstanceOf[Analyser[Any]]))

  def loadPredefinedSerialiser(className:String):AggregateSerialiser = {
    Class.forName(className).getConstructor().newInstance().asInstanceOf[AggregateSerialiser]
  }
}
