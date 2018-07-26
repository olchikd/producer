package com.olchik.producer


object PlayApp extends App {
  println("Playing with scala")

  def reduceT[T](seq: Seq[T], fn:(T, T) => T): T = {
    val iter = seq.iterator
    if (seq.isEmpty) {
      null.asInstanceOf[T]
    } else {
      var result = iter.next()
      while (iter.hasNext) {
        result = fn(result, iter.next())
      }
      result
    }
  }

  def mapT[T](seq: Seq[T], fn:(T) => T): Seq[T] = {
    for (el <- seq) yield fn(el)
  }

  val reduce2: (Seq[Int], (Int, Int) => Int) => Int = (seq, fn) => {
    var result: Int = 0
    seq.foreach(x => {
      result = fn(result, x)
    })
    result
  }

  val numbers = (1 to 10).toSeq
  println("Standart: " + numbers.reduce(_ + _))

  val reduce = reduceT[Int] _
  println(s"My reduce with $numbers: " + reduce(numbers, _ + _))
  println("My reduce with []: " + reduce(Nil, _ + _))

  val map = mapT[Int] _
  println(s"My map with $numbers: " + map(numbers, _ + 10))
  println("My map with []: " + map(Nil, _ + 10))
}

