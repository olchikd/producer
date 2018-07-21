package com.olchik.producer

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}

import scala.annotation.tailrec
import scala.collection.JavaConverters._


object ConsumerApp extends App {
  // https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
  println("Initializing Kafka messages consumer")

  val props = new Properties()
  props.put("bootstrap.servers", AppConfig.KafkaServer)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Seq("books", "purchases").asJava)

  @tailrec
  def fetchRecords(): Seq[Any] = {
    consumer
      .poll(1000)
      .asScala
      .foreach(printKafkaRecord)

    fetchRecords()
  }

  def printKafkaRecord(record: ConsumerRecord[String, String]): Unit = {
    record.topic match {
      case "books" => {
        println(Console.BLUE + s"Supply: '${record.key}', value '${record.value}'")
      }
      case "purchases" => {
        println(Console.GREEN + s"Purchase: '${record.key}', value '${record.value}'")
      }
    }
  }

  fetchRecords()
}
