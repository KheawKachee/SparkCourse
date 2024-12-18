from pyspark.sql import SparkSession,functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")
                                                    #Automatically infers the data types of each column based on the contents.

print("Here is our inferred schema:")
people.printSchema()


friendsByAge = people.select("age","friends")
people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("avgFriends"))\
    .orderBy("age").show(100)

spark.stop()

