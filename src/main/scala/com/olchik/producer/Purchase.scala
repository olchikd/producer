package com.olchik.producer


case class Purchase(asin: String, name: String, count: Int, createdAt: String) extends KafkaObject

object Purchase {
  val people = Vector("Kolia", "Olchik", "Sofia", "Slavko", "Mih")
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")

  def forBook(book: Book): Purchase = {
    Purchase(
      asin=book.asin,
      name=RandomUtils.fromSeq(people).toString,
      count= RandomUtils.notMoreThan(book.rating),
      createdAt=timeFormater.format(new java.util.Date))
  }
}
