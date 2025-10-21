import random
import os
import re
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import StringType

LOG_FILE = "./bluesky.jsonl"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BlueskyHashtags") \
    .getOrCreate()

@udf(StringType())
def get_random_log_line():
    """Returns a random line from the log file without reading the entire file into memory."""
    try:
        if not os.path.exists(LOG_FILE):
            return None  # Handle the case where the log file is missing

        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return None  # Handle empty file scenario

        with open(LOG_FILE, "r") as lf:
            while True:
                random_position = random.randint(0, file_size - 1)  # Pick a random position
                lf.seek(random_position)  # Jump to that position
                lf.readline()  # Discard partial line (move to next full line)
                line = lf.readline().strip()  # Read a full line
                
                if line:  # Ensure we get a valid line
                    return line

    except Exception as e:
        print(str(e))
        return None

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, json_line: str):
        """Extracts hashtags from the input text."""
        try:
            data = json.loads(json_line)
            text = data.get("text", "")
            if text:
                hashtags = re.findall(r"#\w+", text)
                for hashtag in hashtags:
                    yield (hashtag.lower(),)
        except:
            yield None
            
# Register the UDTF for use in Spark SQL
spark.udtf.register("extract_hashtags", HashtagExtractor)

# Create Streaming DataFrame from rate source
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load()

# Enrich DataFrame with canned log data
postLines = rate_df.withColumn("json", get_random_log_line())

# Register DataFrame as a temporary table
postLines.createOrReplaceTempView("raw_posts")

# Use SQL to extract hashtags
hashtags_query = """
    SELECT
        hashtag
    FROM raw_posts,
    LATERAL extract_hashtags(json)
"""

hashtagsDF = spark.sql(hashtags_query)

# Register hashtagsDF as a SQL table for further queries
hashtagsDF.createOrReplaceTempView("hashtags")

# SQL Query: Keep track of the top hashtags encountered over time
topHashtagsDF = spark.sql("""
    SELECT hashtag, COUNT(*) as count 
    FROM hashtags 
    WHERE hashtag IS NOT NULL 
    GROUP BY hashtag 
    ORDER BY count DESC 
    LIMIT 10
""")

# Kick off our streaming query, dumping top user agents to the console
query = (topHashtagsDF.writeStream
         .outputMode("complete")
         .format("console")
         .queryName("top_user_agents")
         .start())

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
