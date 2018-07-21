package com.olchik.producer

/**
  * Using http://github.com/uchidalab/book-dataset
  * "[AMAZON INDEX (ASIN)}","[FILENAME]","[IMAGE URL]","[TITLE]","[AUTHOR]","[CATEGORY ID]","[CATEGORY]"
  */
case class Book(asin: String, title: String, author: String, categoryID: Int, genAt: String, rating: Int) extends KafkaObject {
  override def toString: String = title
}

object Book {
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")
  def fromSeq(item: Seq[String]): Book = {
    Book(
      asin=item(0),
      title=item(3),
      author=item(4),
      categoryID=item(5).toInt,
      genAt=timeFormater.format(new java.util.Date),
      rating = RandomUtils.notMoreThan(5))
  }
}
