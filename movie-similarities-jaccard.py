from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeJaccardSimilarity(spark, data):
    # For Jaccard similarity, we're interested in just the presence of ratings, not their values
    # We'll consider the "set" of movies each user has rated
    
    # Group by movie pairs and count ratings
    jaccardPairs = data \
        .groupBy("movie1", "movie2") \
        .agg(func.count("*").alias("intersection_size"))
    
    # Get counts for each movie (how many users rated each movie)
    movie1Counts = data.groupBy("movie1") \
        .agg(func.countDistinct("userId").alias("movie1_count"))
    
    movie2Counts = data.groupBy("movie2") \
        .agg(func.countDistinct("userId").alias("movie2_count"))
    
    # Join all data together
    jaccardData = jaccardPairs \
        .join(movie1Counts, "movie1") \
        .join(movie2Counts, "movie2")
    
    # Calculate Jaccard similarity: intersection size / (size of A + size of B - intersection size)
    result = jaccardData \
        .withColumn("union_size", 
                   (func.col("movie1_count") + func.col("movie2_count") - func.col("intersection_size"))) \
        .withColumn("score", 
                   func.when(func.col("union_size") > 0, 
                            func.col("intersection_size") / func.col("union_size")) \
                   .otherwise(0)) \
        .select("movie1", "movie2", "score", "intersection_size") \
        .withColumnRenamed("intersection_size", "numPairs")
    
    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities_Jaccard").master("local[*]").getOrCreate()

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

# For Jaccard, we're more interested in the fact that a user rated a movie at all,
# rather than the specific rating value
ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# For Jaccard, we care about which movies are rated together, not the ratings themselves
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.userId").alias("userId"))


moviePairSimilarities = computeJaccardSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    # Jaccard similarity values are typically lower than cosine or Pearson
    # because they're based on set overlap, not rating correlation
    scoreThreshold = 0.4  
    coOccurrenceThreshold = 20.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print("Top 10 similar movies for " + getMovieName(movieNames, movieID) + " (using Jaccard similarity)")
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tco-raters: " + str(result.numPairs))

# Key improvements:
# 1. Implemented Jaccard similarity coefficient, which measures similarity between finite sample sets
# 2. The Jaccard coefficient is calculated as: |A ∩ B| / |A ∪ B|
#    (the size of the intersection divided by the size of the union of the sets)
# 3. This approach treats ratings as binary (rated/not rated) rather than using rating values
# 4. Advantages of Jaccard similarity:
#    - Less sensitive to rating values, focuses on overlapping interests
#    - Works well when comparing user preference patterns regardless of rating scale
#    - Better for sparse datasets where the presence of a rating is more important than its value
# 5. We use a much lower threshold (0.4) since Jaccard scores are typically lower than cosine or Pearson
# 6. This approach is useful for discovering movies with similar audiences
# 7. It performs better when trying to find movies that appeal to the same viewers