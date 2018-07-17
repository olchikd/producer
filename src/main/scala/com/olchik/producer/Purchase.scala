package com.olchik.producer

import java.util.Random
import java.util.UUID.randomUUID

import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson}


case class Purchase(
  asin: String,
  name: String,
  count: Int,
  createdAt: String)


object PurchaseItemGenerator extends ItemGenerator {
  val people = Vector("Kolia", "Olchik", "Sofia", "Slavko", "Mih")
  val rand = new Random(System.currentTimeMillis())

  implicit val formats = DefaultFormats // for liftweb json
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")

  /**
    * Generates tuple with key as ID and value as json string with info about the purchase.
    * @return (key, value) pair
    */
  override def genRow(subitems: Seq[String]): (String, String) = {
    val purchase = Purchase(
      asin=randomItem(subitems),
      name=randomItem(people),
      count=rand.nextInt(3) + 1,
      createdAt=timeFormater.format(new java.util.Date))
    (randomUUID().toString, writeJson(purchase))
  }

  def randomItem(seq: Seq[String]) = seq(rand.nextInt(seq.length))
}
