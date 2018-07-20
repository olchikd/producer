package com.olchik.producer

import java.util.Random
import scala.collection.mutable.ArrayBuffer

import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson}


trait ItemGenerator {
  def genRow: Any

  def genRow(subitems: Seq[Any]): Any
}


object ProducerApp extends App with ProducerMixin {
  println("Initialize Kafka messages producer")
  implicit val formats = DefaultFormats // for liftweb json
  val publishedBooks = new ArrayBuffer[Book]()
  val DelayMs = 500

  while (true) {
    // Publishing new books
    val book = BookItemGenerator.genRow.asInstanceOf[Book]
    writeToKafka(book.asin, writeJson(book), "Publish the book", "books")
    publishedBooks.append(book)
    Thread.sleep(DelayMs)

    val purchase = PurchaseItemGenerator.genRow(publishedBooks).asInstanceOf[Purchase]
    writeToKafka(purchase.asin, writeJson(purchase), "Purchase the book", "purchases")
    Thread.sleep(500)
  }

}
