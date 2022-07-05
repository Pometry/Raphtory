package com.raphtory.api.analysis.graphstate

import scala.collection.mutable

abstract class Counter[T] {

  def totalCount : Int

  def getCounts : mutable.Map[T,Int]

  def largest : (T,Int)

  def largest(k: Int): List[(T,Int)]

}
