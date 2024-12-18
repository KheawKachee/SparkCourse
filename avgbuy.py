from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("AvgBuy").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("price", FloatType(), True)])

df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

customerAvgBuy = df.groupBy("customerID").agg(func.round(func.avg("price"),2).alias("avg price")).orderBy("customerID")
customerAvgBuy.show()

spark.stop()