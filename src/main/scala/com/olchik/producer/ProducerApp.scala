package com.olchik.producer

import java.util.UUID.randomUUID


object ProducerApp extends App with KafkaMixin {
  println("Initialize Kafka messages producer")

  try {
    while (true)
      writeAndSleep()
  }
  finally {
    producer.close()
  }

  def writeAndSleep(interval: Long=1000): Unit = {
    writeToKafka(genKey, genMessage)
    Thread.sleep(interval)
  }

  def genKey = randomUUID().toString
  def genMessage(): String = {
    val now = new java.util.Date
    val formater = new java.text.SimpleDateFormat("mm:hh:ss")
    s"message at ${formater.format(now)}"
  }
}