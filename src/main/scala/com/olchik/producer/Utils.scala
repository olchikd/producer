package com.olchik.producer


object RandomUtils {
  val rand = new scala.util.Random
  def fromSeq(seq: Seq[Any]) = seq(rand.nextInt(seq.length))
  def notMoreThan(maxVal: Int) = rand.nextInt(maxVal) + 1
}