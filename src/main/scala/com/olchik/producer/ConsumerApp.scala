package com.olchik.producer

import scala.collection.JavaConverters._


object ConsumerApp extends App with ConsumerMixin {
  // https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
  println("Initializing Kafka messages consumer")
  while (true) {
    val records = fetchData()
    if (records.nonEmpty) {
      println(s"Got ${records.size} records:")
      for (record <- records) {
        println(s"'${record.key}', value '${record.value}'")
      }
    }
  }
}
