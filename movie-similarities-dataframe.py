from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType,DateType
import sys

def jaccard(spark, data):
    scores = data \
      .withColumn("intersect", func.when(func.col("rating1") == func.col("rating2"), 1).otherwise(0)) \
      .withColumn("union", func.lit(1))\
      .groupBy("movie1", "movie2") \
      .agg(
          func.sum("intersect").alias("intersection_count"),
          func.sum("union").alias("union_count")
      ) \
      .withColumn("jaccard_score", func.col("intersection_count") / func.col("union_count"))  # Jaccard similarity
    return scores
      


# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([\
                              StructField("movieID", IntegerType(), True), 
                              StructField("movieTitle", StringType(), True)
                               ])
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("file:///home/kheaw/sparkcourse/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("file:///home/kheaw/sparkcourse/ml-100k/u.data")

ratings=movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))

ratingThreshold = 3
filteredMoviePairs = moviePairs.filter((moviePairs.rating1 > ratingThreshold) & (moviePairs.rating1 > ratingThreshold))
#filter out rating lower than 5

movieJaccard = jaccard(spark,filteredMoviePairs).cache()
#print(result.collect())



'''
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()
'''
if (len(sys.argv) > 1): #put it when run the code
    scoreThreshold = 0.5
    coOccurrenceThreshold = 35

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = movieJaccard.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("jaccard_score") > scoreThreshold) & (func.col("union_count") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("jaccard_score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.jaccard_score) + "\tstrength: " + str(result.union_count))
    


