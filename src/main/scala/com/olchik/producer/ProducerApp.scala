package com.olchik.producer

import java.util.UUID.randomUUID


trait ItemGenerator {
  def genRow: (String, String) = (randomUUID().toString, "message")
}


object ProducerApp extends App with ProducerMixin {
  println("Initialize Kafka messages producer")

  while (true)
    writeAndSleep()

  def writeAndSleep(interval: Long=1000): Unit = {
    val (key, value) = AppConfig.CurrItemGenerator.genRow
    writeToKafka(key, value)
    Thread.sleep(interval)
  }
}
