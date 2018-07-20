package com.olchik.producer

import java.io.File
import com.github.tototoshi.csv._


case class Book(
  asin: String,
  title: String,
  author: String,
  categoryID: Int,
  genAt: String,
  rating: Int)


/**
  * Using http://github.com/uchidalab/book-dataset
  * "[AMAZON INDEX (ASIN)}","[FILENAME]","[IMAGE URL]","[TITLE]","[AUTHOR]","[CATEGORY ID]","[CATEGORY]"
  */
object BookGenerator extends ItemGenerator {
  val reader = CSVReader.open(new File("./book32-listing.csv"))
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")

  /**
    * Publish a book from csv file.
    * @return the book.
    */
  override def genRow: Any = {
    val list = reader.readNext()
    list match {
      case Some(item) => {
        Book(
          asin=item(0),
          title=item(3),
          author=item(4),
          categoryID=item(5).toInt,
          genAt=timeFormater.format(new java.util.Date),
          rating = RandomUtils.notMoreThan(5))
      }
      case None => genRow
    }
  }

  def genRow(subitems: Seq[Any]): Any = genRow
}


