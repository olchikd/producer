Zeppeling Notebook
==================

Working with static data
------------------------
Loading data from CSV file:

    %spark
    import spark.implicits._
    val source = scala.io.Source.fromURL("https://raw.githubusercontent.com/olchikd/producer/master/categories.csv")
    val categories = (for (line <- source.getLines) yield {
      val items = line.split(",")
      (items(0).toInt, items(1).toString)
    }).toSeq
    
    val categoriesDf = categories.toDF("id", "name").
      selectExpr("cast(id as int) id", "name")
    z.show(categoriesDf)


Structured streaming
--------------------

Create stream and join with static data:

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    
    // Config
    val Topic = "test"
    val KafkaServer = "localhost:9092"
    val streamingDf = spark.
        readStream.format("kafka").
        option("kafka.bootstrap.servers", KafkaServer).
        option("subscribe", Topic).
        load()
        
    val schema: StructType = new StructType().
      add("asin", "string").
      add("title", "string").
      add("author", "string").
      add("categoryID", "integer").
      add("genAt", "string")
    
    println(s"Is streaming: ${streamingDf.isStreaming}.")
    val query = streamingDf.
        select(
            $"key".cast(StringType), 
            $"value".cast(StringType), 
            get_json_object(($"value").cast("string"), "$.name").alias("name"),
            from_json($"value".cast(StringType), schema).alias("valuejson"),
            $"timestamp").
        join(categoriesDf.withColumnRenamed("name", "category"), categoriesDf.col("id") === $"valuejson.categoryID").
        writeStream.
        format("memory").
        queryName("messages").
        start()
    spark.table("messages").printSchema

Displaying results:

    z.show(spark.sql("select key, valuejson.title, valuejson.author, category, timestamp from messages"))