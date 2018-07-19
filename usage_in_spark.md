Zeppeling Notebook
==================

Join Stream with static data
----------------------------

There are books CSV file and publishing books stream.

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
    z.show(spark.sql("select key, valuejson.title, valuejson.author, category, timestamp from messages"))
    
Join publishing and purchasing books streams by book id asin
------------------------------------------------------------

    %spark
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    
Config

    val KafkaServer = "localhost:9092"
    val schema: StructType = new StructType().
      add("asin", "string").
      add("name", "string").
      add("count", "integer").
      add("createdAt", "string")
      
Books purchasing stream

    val purchaseDf = spark.
        readStream.format("kafka").
        option("kafka.bootstrap.servers", KafkaServer).
        option("subscribe", "purchases").
        load().
        select(
            $"key".cast(StringType), 
            $"value".cast(StringType),
            $"timestamp".alias("purchaseTimestamp"),
            get_json_object(($"value").cast("string"), "$.asin").alias("bookAsin"),
            get_json_object(($"value").cast("string"), "$.name").alias("person"),
            from_json($"value".cast(StringType), schema).alias("purchasejson")).
        withWatermark("purchaseTimestamp", "100 seconds")
    
    
Publishing books stream (join with static data with book's categories)

    val booksDf = spark.
        readStream.format("kafka").
        option("kafka.bootstrap.servers", KafkaServer).
        option("subscribe", "books").
        load()
        
    val schema: StructType = new StructType().
      add("asin", "string").
      add("title", "string").
      add("author", "string").
      add("categoryID", "integer").
      add("genAt", "string")
    
    val query = booksDf.
        select(
            $"key".cast(StringType), 
            $"value".cast(StringType),
            from_json($"value".cast(StringType), schema).alias("bookjson"),
            get_json_object(($"value").cast("string"), "$.asin").alias("asin"),
            get_json_object(($"value").cast("string"), "$.categoryID").alias("categoryID"),
            $"timestamp".alias("bookTimestamp")).
        withWatermark("bookTimestamp", "100 seconds").
        join(categoriesDf.withColumnRenamed("name", "category"), categoriesDf.col("id") === $"categoryID").
        join(purchaseDf, expr("""
          asin = bookAsin AND
          purchaseTimestamp >= bookTimestamp AND
          purchaseTimestamp <= bookTimestamp + interval 1 hour
          """)).
        writeStream.
        format("memory").
        queryName("books").
        start()
    spark.table("books").printSchema
    
    z.show(spark.sql("select * from books"))
    
Configuring sending metrics to Graphite
---------------------------------------

Install graphana and graphite:

    https://github.com/hazelcast/docker-grafana-graphite

Add spark interpreter setting in Zeppelin:
    
    spark.metrics.conf	/Users/olchik/workspace/home/producer/metrics.properties

Enable sending metrics

    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    
Grafana dashboard 

    http://localhost/
    
Standard user/password:  "admin:admin"