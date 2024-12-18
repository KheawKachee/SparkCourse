from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName('MovieRating').getOrCreate()
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", IntegerType(), True)])

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create a user-defined function to look up movie names from our broadcasted dictionary
lookupNameUDF = func.udf(lambda x : nameDict.value[x])
#cant nameDict[x] cause it isnt dict but broadcast var

movierating = spark.read.option("sep","\t").schema(schema).csv("ml-100k/u.data")

ranking = movierating.groupBy("movieID").agg(func.count("movieID").alias("user engage"))\
    .orderBy("user engage",ascending = False)\
    .withColumn("title", lookupNameUDF("movieID"))\
    .select("movieID","title","user engage")

ranking.show(10,truncate = False)
spark.stop()