from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, ArrayType
import sys

def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
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

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities_Genre").master("local[*]").getOrCreate()

# Create schema for movie data
movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True), \
                               StructField("releaseDate", StringType(), True), \
                               StructField("videoReleaseDate", StringType(), True), \
                               StructField("imdbUrl", StringType(), True), \
                               StructField("unknown", BooleanType(), True), \
                               StructField("Action", BooleanType(), True), \
                               StructField("Adventure", BooleanType(), True), \
                               StructField("Animation", BooleanType(), True), \
                               StructField("Children", BooleanType(), True), \
                               StructField("Comedy", BooleanType(), True), \
                               StructField("Crime", BooleanType(), True), \
                               StructField("Documentary", BooleanType(), True), \
                               StructField("Drama", BooleanType(), True), \
                               StructField("Fantasy", BooleanType(), True), \
                               StructField("FilmNoir", BooleanType(), True), \
                               StructField("Horror", BooleanType(), True), \
                               StructField("Musical", BooleanType(), True), \
                               StructField("Mystery", BooleanType(), True), \
                               StructField("Romance", BooleanType(), True), \
                               StructField("SciFi", BooleanType(), True), \
                               StructField("Thriller", BooleanType(), True), \
                               StructField("War", BooleanType(), True), \
                               StructField("Western", BooleanType(), True) \
                               ])

moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID, movieTitle, and genres
# Load the u.item file, handling the 24 fields
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

# Compute base similarity scores
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

# Define genre columns - match the Boolean fields in the schema
genreColumns = ["Action", "Adventure", "Animation", "Children", "Comedy", 
               "Crime", "Documentary", "Drama", "Fantasy", "FilmNoir", 
               "Horror", "Musical", "Mystery", "Romance", "SciFi", 
               "Thriller", "War", "Western"]

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0
    genreBoostFactor = 0.1  # Boost for each matching genre

    movieID = int(sys.argv[1])

    # Get genre information for the target movie
    targetMovie = movieNames.filter(func.col("movieID") == movieID).select(*genreColumns).collect()[0]
    
    # Filter for potential similar movies
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Convert to a list of Row objects
    resultsWithoutBoost = filteredResults.sort(func.col("score").desc()).collect()
    
    # Now we'll apply genre boost manually for each result
    resultsWithGenreBoost = []
    
    for result in resultsWithoutBoost:
        # Get the ID of the other movie
        otherMovieID = result.movie1
        if (otherMovieID == movieID):
            otherMovieID = result.movie2
        
        # Get genre info for the other movie
        otherMovie = movieNames.filter(func.col("movieID") == otherMovieID).select(*genreColumns).collect()[0]
        
        # Count matching genres
        matchingGenres = sum(1 for i in range(len(genreColumns)) 
                            if targetMovie[i] and otherMovie[i])
        
        # Apply genre boost to the score (up to a maximum total score of 1.0)
        genreBoost = matchingGenres * genreBoostFactor
        boostedScore = min(1.0, result.score + genreBoost)
        
        # Create a result entry with all the information
        resultsWithGenreBoost.append({
            "movieID": otherMovieID,
            "baseScore": result.score,
            "genreBoost": genreBoost,
            "finalScore": boostedScore,
            "numPairs": result.numPairs,
            "matchingGenres": matchingGenres
        })
    
    # Sort the results by boosted score
    resultsWithGenreBoost.sort(key=lambda x: x["finalScore"], reverse=True)
    
    print("Top 10 similar movies for " + getMovieName(movieNames, movieID) + " (with genre boost)")
    
    # Print the top 10 results
    for i, result in enumerate(resultsWithGenreBoost[:10]):
        otherMovieID = result["movieID"]
        print(f"{i+1}. {getMovieName(movieNames, otherMovieID)}")
        print(f"   Base similarity: {result['baseScore']:.4f}")
        print(f"   Genre boost: {result['genreBoost']:.4f} ({result['matchingGenres']} matching genres)")
        print(f"   Final score: {result['finalScore']:.4f}")
        print(f"   Co-ratings: {result['numPairs']}")

# Key improvements:
# 1. Added genre information by loading all fields from the u.item file
# 2. Calculated a genre boost based on the number of shared genres between movies
# 3. Applied a boost factor of 0.1 per matching genre to the similarity score
# 4. Benefits of using genre information:
#    - Reduces algorithmic bias from rating patterns alone
#    - Helps address the "filter bubble" problem by boosting thematically similar content
#    - Improves recommendations for niche genres with fewer ratings
#    - Gives context-aware recommendations that align with user interests
# 5. Additional improvements possible:
#    - Different weights could be applied to different genres
#    - Genre popularity could be considered (rare genres could have higher weight)
#    - Combine with other improvements (good ratings filter, weighted similarity)
# 6. This approach gives explainable recommendations:
#    - "We recommended this because it shares these genres with movies you liked"
#    - Transparency improves user trust in the recommendation system
# 7. The genre boost can be tuned via the genreBoostFactor parameter