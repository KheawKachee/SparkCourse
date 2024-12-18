from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs


spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

def getNames():
    name = {}
    with codecs.open("marvel/Marvel+Names", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split(" ",1)
            name[fields[0]] = fields[1][1:-3]
    return name

lines = spark.read.text("marvel/Marvel+Graph")

nameDict = spark.sparkContext.broadcast(getNames())
udf_getName = func.udf(lambda x:nameDict.value[x])

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \

"""
connections.groupBy("id").agg(func.sum("connections").alias("connections"))\
    .orderBy("connections",ascending =False).withColumn("name",udf_getName("id"))\
    .withColumn("name", func.trim(func.col("name")))\
    .select("name","connections","id").show(20,truncate=False)


"""
minConnections = connections.select(func.min("connections")).collect()[0][0]
connections.filter(connections.connections == minConnections).groupBy("id").agg(func.sum("connections").alias("connections"))\
    .orderBy("connections",ascending =False).withColumn("name",udf_getName("id"))\
    .withColumn("name", func.trim(func.col("name")))\
    .select("id","name","connections").show(20,truncate=False)

