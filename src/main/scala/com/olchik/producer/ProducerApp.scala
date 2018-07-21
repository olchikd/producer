package com.olchik.producer

import java.util.Random

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Success,Failure}
import scala.collection.mutable.ArrayBuffer

import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson}


trait ItemGenerator {
  def genRow: Any

  def genRow(subitems: Seq[Any]): Any
}


object ProducerApp extends App with ProducerMixin {
  println("Initialize Book shop emulator")
  val publishedBooks = new ArrayBuffer[Book]()
  implicit val executor =  scala.concurrent.ExecutionContext.global
  implicit val formats = DefaultFormats // for liftweb json

  def bookSupply(): Future[Book] = Future {
    val book = BookGenerator.genRow.asInstanceOf[Book]
    writeToKafka(book.asin, writeJson(book), "books")
    println(Console.BLUE + s"Supply: '${book.title}' (${book.asin}, category: ${book.categoryID})")
    publishedBooks.append(book)
    if (publishedBooks.lengthCompare(100) > 0) {
      publishedBooks.remove(0)
    }
    sleep()
    book
  }

  def bookPuchase(): Future[Purchase] = Future {
    val purchase = PurchaseGenerator.genRow(publishedBooks).asInstanceOf[Purchase]
    writeToKafka(purchase.asin, writeJson(purchase), "purchases")
    println(Console.GREEN + s"Purchase: '${purchase.asin}' by '${purchase.name}'")
    sleep()
    purchase
  }

  def sleep(delayMean: Int = 500, delayDisp: Int = 10): Unit ={
    val delay = delayMean - delayDisp + RandomUtils.notMoreThan(2 * delayDisp)
    println(s"Sleeping $delay ms")
    Thread.sleep(delay)
  }

  def shopLoop() : Future[Any] =  {
    RandomUtils.notMoreThan(2) match {
      case 1 => bookSupply.flatMap(_ => shopLoop())
      case 2 => bookPuchase.flatMap(_ => shopLoop())
    }
  }

  Await.result(shopLoop(), Duration.Inf)
}
