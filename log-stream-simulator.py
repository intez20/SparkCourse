import random
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_extract

# Define absolute path to the log file
LOG_FILE = os.path.join(os.getcwd(), "access_log.txt")

# Check if the log file exists before proceeding
if not os.path.exists(LOG_FILE):
    print(f"ERROR: Log file not found: {LOG_FILE}")
    print("Please make sure access_log.txt is in the current directory.")
    sys.exit(1)

# Initialize Spark Session with Windows-friendly configuration
spark = SparkSession.builder \
    .appName("SparkStreamingLogSimulator") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# Pre-load log lines into memory to avoid file IO during streaming
# This avoids the native IO issues on Windows
print(f"Loading log data from {LOG_FILE}...")
log_lines = []
try:
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as lf:
        log_lines = [line.strip() for line in lf if line.strip()]
    
    if not log_lines:
        print("ERROR: No valid lines found in the log file")
        sys.exit(1)
        
    print(f"Successfully loaded {len(log_lines)} log lines")
except Exception as e:
    print(f"ERROR loading log file: {str(e)}")
    sys.exit(1)

@udf(StringType())
def get_random_log_line():
    """Returns a random line from the pre-loaded log lines."""
    try:
        return random.choice(log_lines)
    except Exception as e:
        print(f"Error in get_random_log_line: {str(e)}")
        return "127.0.0.1 - - [21/Oct/2025:12:00:00 -0400] \"GET /default HTTP/1.1\" 200 1234"  # Fallback line
    
# Create Streaming DataFrame from rate source
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) .load()
    
# Enrich DataFrame with canned log data
accessLines = rate_df \
    .withColumn("value", get_random_log_line())
    
# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access by status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

try:
    print("Starting streaming query...")
    
    # Kick off our streaming query, dumping results to the console
    query = statusCountsDF.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .queryName("counts") \
        .start()
    
    print("Streaming query started successfully!")
    print("Processing log data... (Press Ctrl+C to terminate)")
    
    # Run for a limited time or until manually terminated
    # This helps with testing and prevents hanging
    timeout_seconds = 60  # Adjust as needed
    query.awaitTermination(timeout_seconds)
    print(f"\nQuery completed after {timeout_seconds} seconds.")
    
except KeyboardInterrupt:
    print("\nStreaming terminated by user")
except Exception as e:
    print(f"\nERROR in streaming query: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    # Clean shutdown in any scenario
    try:
        if 'query' in locals() and query is not None:
            print("Stopping streaming query...")
            query.stop()
            print("Query stopped.")
    except:
        pass
        
    print("Stopping Spark session...")
    spark.stop()
    print("Spark session stopped. Application complete.")