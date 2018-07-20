package com.olchik.producer


case class Purchase(
  asin: String,
  name: String,
  count: Int,
  createdAt: String)


object PurchaseGenerator extends ItemGenerator {
  val people = Vector("Kolia", "Olchik", "Sofia", "Slavko", "Mih")
  val timeFormater = new java.text.SimpleDateFormat("mm:hh:ss")

  /**
    * No books in the store, but we can buy a pen! :)
    * @return Purchase with a pen.
    */
  override def genRow: Any = {
    Purchase(
      asin="pen_asin",
      name="a pen",
      count=1,
      createdAt=timeFormater.format(new java.util.Date)
    )
  }

  /**
    * Generates the random book purchase.
    * Note: Emulates book count field depends on book's rating.
    * @return Purchase with a book / books
    */
  override def genRow(subitems: Seq[Any]): Any = {
    // no books in the store
    if (subitems.isEmpty) return genRow
    val book = RandomUtils.fromSeq(subitems).asInstanceOf[Book]
    Purchase(
      asin=book.asin,
      name=RandomUtils.fromSeq(people).toString,
      count= RandomUtils.notMoreThan(book.rating),
      createdAt=timeFormater.format(new java.util.Date))
  }
}
