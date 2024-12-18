from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType,DateType

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .csv("file:///home/kheaw/sparkcourse/ml-100k/u.item")

