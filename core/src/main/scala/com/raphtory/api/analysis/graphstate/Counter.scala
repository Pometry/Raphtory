package com.raphtory.api.analysis.graphstate

abstract class Counter[T] {

  def totalCount : Int

  def getCounts : Map[T,Int]

  def largest : (T,Int)

  def largest(k: Int): List[(T,Int)]

}
