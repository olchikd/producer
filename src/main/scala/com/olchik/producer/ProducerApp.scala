package com.olchik.producer

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Success,Failure}
import scala.collection.mutable.ArrayBuffer

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

import java.io.File
import com.github.tototoshi.csv._
import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson}


object ProducerApp extends App {
  println("Initialize Book shop emulator")
  implicit val executor =  scala.concurrent.ExecutionContext.global
  implicit val formats = DefaultFormats // for liftweb json

  val props = new Properties()
  props.put("bootstrap.servers", AppConfig.KafkaServer)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def writeToKafka(key: String, value: String, topic: String = "test"): Unit = Future {
    producer.send(new ProducerRecord(topic, key, value))
  }

  CSVReader.open(new File("./book32-listing.csv"))
    .iterator
    .map(Book.fromSeq)
    .grouped(10).foreach(supply => {
      // New supply of the 10 books.
      supply.foreach(matchAndWrite)
      // Let's start buying them! Random people go and buy random 20 books.
      (1 to 20)
        .map(i => Purchase.forBook(RandomUtils.fromSeq(supply).asInstanceOf[Book]))
        .foreach(matchAndWrite)
    })

  def matchAndWrite(obj: KafkaObject): Unit = {
    obj match {
      case book: Book => {
        println(Console.BLUE + s"Supply: '${book.title}' (${book.asin}, category: ${book.categoryID})")
        writeToKafka(book.asin, writeJson(book), "books")
      }
      case purchase: Purchase => {
        println(Console.GREEN + s"Purchase: '${purchase.asin}' by '${purchase.name}'")
        writeToKafka(purchase.asin, writeJson(purchase), "purchases")
      }
    }
    sleep()
  }

  def sleep(delayMean: Int = 4, delayDisp: Int = 2): Unit ={
    val delay = delayMean - delayDisp + RandomUtils.notMoreThan(2 * delayDisp)
    // println(s"Sleeping $delay ms")
    Thread.sleep(delay)
  }
}

trait KafkaObject {
}

