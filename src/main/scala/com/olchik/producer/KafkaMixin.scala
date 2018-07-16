package com.olchik.producer

import java.util.Properties

import org.apache.kafka.clients.producer._


trait KafkaMixin {
  val  props = new Properties()
  props.put("bootstrap.servers", AppConfig.KafkaServer)

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def writeToKafka(key: String, value: String, topicOption: Option[String] = None): Unit = {
    val topic = topicOption match {
      case Some(t) => t
      case None => AppConfig.KafkaTopic
    }
    val record = new ProducerRecord(topic, key, value)
    println(s"Sending key: '${record.key}', value '${record.value}'")
    producer.send(record)
  }
}