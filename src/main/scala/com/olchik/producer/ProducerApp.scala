package com.olchik.producer

import scala.collection.mutable.ArrayBuffer
import java.util.UUID.randomUUID


trait ItemGenerator {
  def genRow: (String, String) = (randomUUID().toString, "message")

  def genRow(subitems: Seq[String]): (String, String) = genRow
}


object ProducerApp extends App with ProducerMixin {
  println("Initialize Kafka messages producer")
  val publishedBooks = new ArrayBuffer[String]()

  while (true) {
    // Publishing new books
    val (id, book) = BookItemGenerator.genRow
    writeToKafka(id, book, "Publish the book")
    publishedBooks.append(id)
    Thread.sleep(500)

    val (purchaseId, purchase) = BookItemGenerator.genRow(publishedBooks)
    writeToKafka(purchaseId, purchase, "Purchase the book", "purchases")
    Thread.sleep(500)
  }

}
