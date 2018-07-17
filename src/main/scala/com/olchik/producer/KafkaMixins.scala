package com.olchik.producer

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._


trait KafkaMixin {
  protected val props = new Properties()
  props.put("bootstrap.servers", AppConfig.KafkaServer)

  def close(): Unit
}


trait ProducerMixin extends KafkaMixin {
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  protected val producer = new KafkaProducer[String, String](props)

  def writeToKafka(key: String, value: String,
                   message: String = "Sending key",
                   topic: String = "topic"): Unit = {
    val record = new ProducerRecord(topic, key, value)
    println(s"$message: '${record.key}' - '${record.value}'")
    producer.send(record)
  }

  def close(): Unit = {
    producer.close()
  }
}


trait ConsumerMixin extends KafkaMixin {
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")
  protected val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe((AppConfig.KafkaTopic :: Nil).asJava)

  def fetchData()={
    val records = consumer.poll(1000) // featching data
    records.asScala
  }

  def close(): Unit = {
    consumer.close()
  }
}
