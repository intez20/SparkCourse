from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
import sys
import math

def computeWeightedSimilarity(spark, data):
    # First compute standard cosine similarity
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate base cosine similarity score
    baseSim = calculateSimilarity \
      .withColumn("cosine_score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0))
    
    # Create a confidence factor based on number of co-ratings
    # Using a sigmoid function to create a weight that approaches 1.0 as numPairs increases
    # Define k = 0.04 for the steepness and m = 25 for the midpoint
    weightedResult = baseSim \
      .withColumn("confidence_weight", 
                 1.0 / (1.0 + func.exp(-0.04 * (func.col("numPairs") - 25)))) \
      .withColumn("score", 
                 func.col("cosine_score") * func.col("confidence_weight")) \
      .select("movie1", "movie2", "score", "numPairs", "cosine_score", "confidence_weight")
    
    return weightedResult

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities_WeightedMetric").master("local[*]").getOrCreate()

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


moviePairSimilarities = computeWeightedSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.50  # Lower threshold since weighted scores are generally lower
    coOccurrenceThreshold = 10.0  # We can use a lower threshold since we're already weighting by co-occurrence

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print("Top 10 similar movies for " + getMovieName(movieNames, movieID) + " (using weighted similarity)")
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + 
              f"\tWeighted score: {result.score:.4f}" +
              f"\tBase score: {result.cosine_score:.4f}" +
              f"\tConfidence: {result.confidence_weight:.4f}" +
              f"\tCo-ratings: {result.numPairs}")

# Key improvements:
# 1. Created a weighted similarity metric that factors in the number of co-ratings 
# 2. The approach uses a sigmoid function to create a confidence weight:
#    weight = 1 / (1 + exp(-k * (numPairs - m)))
#    where k controls the steepness and m is the midpoint
# 3. Benefits of this weighted approach:
#    - Discounts similarities based on very few co-ratings
#    - Approaches the true cosine similarity as co-ratings increase
#    - Gives higher confidence to similarities based on more data points
#    - Smooth transition between low and high confidence
# 4. Parameters can be tuned:
#    - k=0.04: controls how quickly confidence increases with more ratings
#    - m=25: the inflection point (50% confidence at this many co-ratings)
# 5. Advantages:
#    - Reduces false positives from coincidental small sample similarities
#    - Gives more weight to statistically significant relationships
#    - Doesn't require arbitrary thresholds
#    - Still returns results even for less popular movies
# 6. This can be combined with other similarity metrics (Pearson, Jaccard)
#    by replacing the cosine_score calculation