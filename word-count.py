from pyspark.sql import SparkSession,functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

book = spark.read.option("header", "false").text("Book")

words = book.select(func.explode(func.split(func.lower(book.value), '\\W+')).alias("word")).filter(func.col("word") != "")


wordsCount = words.groupBy("word").agg(func.count("word").alias("freq")).orderBy("freq",ascending=False).show()

spark.stop()

