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
  println("Initialize Book shop emulator")
  implicit val formats = DefaultFormats // for liftweb json
  val publishedBooks = new ArrayBuffer[Book]()
  val DelayMean = 500
  val DelayDisp = DelayMean / 5
  val generators = Array(BookGenerator, PurchaseGenerator)

  while (true) {
    val generator = RandomUtils.fromSeq(generators)
    generator match {
      case BookGenerator => {
        val book = BookGenerator.genRow.asInstanceOf[Book]
        writeToKafka(book.asin, writeJson(book), "books")
        println(Console.BLUE + s"Supply: '${book.title}' (${book.asin})")
        publishedBooks.append(book)
      }
      case PurchaseGenerator => {
        val purchase = PurchaseGenerator.genRow(publishedBooks).asInstanceOf[Purchase]
        writeToKafka(purchase.asin, writeJson(purchase), "purchases")
        println(Console.GREEN + s"Purchase: '${purchase.asin}' by '${purchase.name}'")
      }
    }
    val delay = DelayMean - DelayDisp + RandomUtils.notMoreThan(2 * DelayDisp)
    println(s"Sleeping $delay ms")
    Thread.sleep(delay)
  }

}
