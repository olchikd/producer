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

    %spark
    // Publishing books stream (join with static data with book's categories)
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
      add("genAt", "string").
      add("rating", "integer")
    
    val resultDf = booksDf.
        select(
            $"key".cast(StringType), 
            $"value".cast(StringType),
            from_json($"value".cast(StringType), schema).alias("bookjson"),
            get_json_object(($"value").cast("string"), "$.asin").alias("asin"),
            get_json_object(($"value").cast("string"), "$.title").alias("title"),
            get_json_object(($"value").cast("string"), "$.categoryID").alias("categoryID"),
            get_json_object(($"value").cast("string"), "$.rating").alias("bookRating"),
            $"timestamp".alias("bookTimestamp")).
        withWatermark("bookTimestamp", "100 seconds").
        join(categoriesDf.withColumnRenamed("name", "category"), categoriesDf.col("id") === $"categoryID").
        join(purchaseDf, expr("""
          asin = bookAsin AND
          purchaseTimestamp >= bookTimestamp AND
          purchaseTimestamp <= bookTimestamp + interval 1 hour
          """))
    val resultDf = bookDf2
    val query = resultDf.writeStream.
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

Training the model
------------------

Let's train the model, that predict CategoryID by book specific information.

Load in dataframe from csv

    %spark
    import org.apache.spark.sql.functions.rand
    // Creating ML datasets for fitting and evaluating the model
    val pathToBooksCsv = "/Users/olchik/workspace/home/producer/book32-listing.csv"
    // "[AMAZON INDEX (ASIN)}","[FILENAME]","[IMAGE URL]","[TITLE]","[AUTHOR]","[CATEGORY ID]","[CATEGORY]"
    val schema = StructType(Array(
            StructField("asin", StringType, true),
            StructField("filename", StringType, true),
            StructField("image", StringType, true),
            StructField("title", StringType, true),
            StructField("author", StringType, true),
            StructField("categoryID", IntegerType, true),
            StructField("category", StringType, true)))
    val readedDf = sqlContext.read.format("csv").option("header", "true").schema(schema).load(pathToBooksCsv)

    def generateDf(count: Int) = readedDf.orderBy(rand()).limit(count)

Fit the model and save

    %spark
    import org.apache.spark.ml.{Pipeline, PipelineModel}
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row
    
    // Tokenize title
    val tokenizer = new Tokenizer().
       setInputCol("title").
       setOutputCol("words")
    val hashingTF = new HashingTF().
       setNumFeatures(1000).
       setInputCol(tokenizer.getOutputCol).
       setOutputCol("features")
    val lr = new LogisticRegression().
       setMaxIter(10).
       setRegParam(0.001).
       setLabelCol("categoryID")
       
    // Tokenize author
    val tokenizerAuthor = new Tokenizer().
       setInputCol("author").
       setOutputCol("words")
    val hashingTFAuthor = new HashingTF().
       setNumFeatures(1000).
       setInputCol(tokenizer.getOutputCol).
       setOutputCol("aFeatures")
     
    val lr = new LogisticRegression().
       setMaxIter(10).
       setRegParam(0.001).
       setLabelCol("categoryID")
       
    val pipeline = new Pipeline().
       setStages(Array(tokenizer, hashingTF, tokenizerAuthor, hashingTFAuthor, lr))
    
    
    val trainingDf = generateDf(200000)
    z.show(trainingDf)
    val model = pipeline.fit(trainingDf)
    
    model.write.overwrite().save("/Users/olchik/workspace/home/categoryModel")

Predict

    val myDf = generateDf(100)
    val predictedCategoryDf = model.transform(myDf).select("title", "author", "categoryID", "prediction") // "probability
    z.show(predictedCategoryDf)
    
Using the model in the stream
-----------------------------

    val booksWithPurchaseDf = ...
    
    import org.apache.spark.ml.PipelineModel
    val model = PipelineModel.load("/Users/olchik/workspace/home/categoryModel")
    val transformedDf = model.transform(booksWithPurchaseDf)
    
    val query = transformedDf.writeStream.
        format("memory").
        queryName("books").
        start()
