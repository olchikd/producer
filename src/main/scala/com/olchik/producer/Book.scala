package com.olchik.producer

import java.io.File

import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson}
import com.github.tototoshi.csv._


case class Book(
  asin: String,
  title: String,
  author: String,
  categoryID: Int,
  genAt: String)


/**
  * Using http://github.com/uchidalab/book-dataset
  * "[AMAZON INDEX (ASIN)}","[FILENAME]","[IMAGE URL]","[TITLE]","[AUTHOR]","[CATEGORY ID]","[CATEGORY]"
  */
object BookItemGenerator extends ItemGenerator {
  val reader = CSVReader.open(new File("./book32-listing.csv"))
  implicit val formats = DefaultFormats // for liftweb json
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")

  /**
    * Generates tuple with key as ID and value as json string with info about book.
    * @return (key, value) pair
    */
  override def genRow: (String, String) = {
    val list = reader.readNext()
    list match {
      case Some(item) => {
        val book = Book(
          asin=item(0),
          title=item(3),
          author=item(4),
          categoryID=item(5).toInt,
          genAt=timeFormater.format(new java.util.Date))
        (item(0), writeJson(book))
      }
      case None => genRow
    }
  }
}


