from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computePearsonSimilarity(spark, data):
    # Calculate statistics for each movie pair
    # First calculate means for each movie
    pairScores = data \
      .groupBy("movie1", "movie2") \
      .agg( \
          func.count(func.col("rating1")).alias("count"),
          func.sum(func.col("rating1")).alias("sum1"),
          func.sum(func.col("rating2")).alias("sum2"),
          func.sum(func.col("rating1") * func.col("rating1")).alias("squareSum1"),
          func.sum(func.col("rating2") * func.col("rating2")).alias("squareSum2"),
          func.sum(func.col("rating1") * func.col("rating2")).alias("dotProduct") \
      )
      
    # Calculate the Pearson correlation coefficient
    pearsonCalc = pairScores \
      .withColumn("numerator", func.col("dotProduct") - (func.col("sum1") * func.col("sum2") / func.col("count"))) \
      .withColumn("denominator1", func.sqrt(func.col("squareSum1") - func.pow(func.col("sum1"), 2.0) / func.col("count"))) \
      .withColumn("denominator2", func.sqrt(func.col("squareSum2") - func.pow(func.col("sum2"), 2.0) / func.col("count")))
    
    # Calculate score and handle divide by zero
    result = pearsonCalc \
      .withColumn("score", \
        func.when((func.col("denominator1") * func.col("denominator2")) != 0, 
                 func.col("numerator") / (func.col("denominator1") * func.col("denominator2"))) \
          .otherwise(0) \
      ) \
      .select("movie1", "movie2", "score", "count")
    
    # Rename count to numPairs to maintain compatibility with the original code
    result = result.withColumnRenamed("count", "numPairs")
    
    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities_Pearson").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
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
      .csv("file:///SparkCourse/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("file:///SparkCourse/ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating")

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


moviePairSimilarities = computePearsonSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.90  # Lower threshold for Pearson as it's stricter
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID) + " (using Pearson correlation)")
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))

# Key improvements:
# 1. We've replaced cosine similarity with Pearson correlation coefficient
# 2. While cosine similarity measures the angle between vectors, Pearson accounts for rating scale differences
# 3. Pearson correlation normalizes ratings by each user's mean rating, removing individual rating biases
# 4. This helps adjust for users who tend to rate everything high or low
# 5. Pearson ranges from -1 to 1, with:
#    - 1 indicating perfect positive correlation
#    - 0 indicating no correlation
#    - -1 indicating perfect negative correlation
# 6. We use a slightly lower threshold (0.90 vs 0.97) because Pearson is often stricter than cosine
# 7. Pearson works better when users have different rating scales (some rate 1-3, others 3-5)