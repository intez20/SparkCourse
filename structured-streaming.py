# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

# Create a SparkSession (the config bit is only for Windows!)
import os
import shutil
import sys
from pyspark.sql.types import StringType, StructType, StructField

# Create a SparkSession with better Windows configuration
spark = SparkSession.builder\
    .config("spark.sql.warehouse.dir", "file:///C:/temp")\
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/temp/checkpoint")\
    .config("spark.ui.enabled", "false")\
    .appName("StructuredStreaming")\
    .master("local[*]")\
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# Use absolute path for the logs directory
# Create a temporary streaming directory
temp_streaming_dir = os.path.join(os.getcwd(), "streaming_logs")
if os.path.exists(temp_streaming_dir):
    shutil.rmtree(temp_streaming_dir)
os.makedirs(temp_streaming_dir)
print(f"Created temporary streaming directory: {temp_streaming_dir}")

# Explicitly define schema for the text input
textSchema = StructType([StructField("value", StringType(), True)])

try:
    # Monitor the temporary directory for new log data with schema
    accessLines = spark.readStream\
        .schema(textSchema)\
        .option("maxFilesPerTrigger", 1)\
        .text(temp_streaming_dir)
    
    print("Successfully created streaming DataFrame")
except Exception as e:
    print(f"Error setting up streaming source: {str(e)}")
    sys.exit(1)

# Parse out the common log format to a DataFrame
try:
    contentSizeExp = r'\s(\d+)$'
    statusExp = r'\s(\d{3})\s'
    generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

    # First, print the schema to debug
    print("Access lines schema:")
    accessLines.printSchema()

    # Use more defensive regexp processing with null handling
    logsDF = accessLines.select(
        regexp_extract('value', hostExp, 1).alias('host'),
        regexp_extract('value', timeExp, 1).alias('timestamp'),
        regexp_extract('value', generalExp, 1).alias('method'),
        regexp_extract('value', generalExp, 2).alias('endpoint'),
        regexp_extract('value', generalExp, 3).alias('protocol'),
        regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
        regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size')
    )
    
    print("Successfully created logs DataFrame")

    # Add error handling for null values
    from pyspark.sql.functions import when, col, lit
    
    # Replace nulls with default values to prevent issues
    logsDF = logsDF.withColumn("status", 
                              when(col("status").isNull(), lit(0))
                              .otherwise(col("status")))
    
    # Keep a running count of every access by status code
    statusCountsDF = logsDF.groupBy(logsDF.status).count()
    print("Successfully created status counts DataFrame")
    
except Exception as e:
    print(f"Error processing log format: {str(e)}")
    sys.exit(1)

try:
    # Kick off our streaming query, dumping results to the console
    print("Starting streaming query...")
    query = statusCountsDF.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
        .option("numRows", 30)\
        .queryName("counts")\
        .start()
    
    print("Query started successfully!")
    
    # After the query has started, copy the log file to the streaming directory
    # to simulate streaming data
    import threading
    import time
    
    def copy_log_file():
        try:
            # Wait a bit for the streaming context to initialize
            time.sleep(5)
            source_log = os.path.join(os.getcwd(), "logs", "access_log.txt")
            
            # Check if source log exists
            if not os.path.exists(source_log):
                print(f"WARNING: Source log file {source_log} does not exist")
                return
                
            # Copy in smaller chunks to simulate streaming
            with open(source_log, 'r') as f:
                lines = f.readlines()
                
            total_lines = len(lines)
            batch_size = min(100, total_lines)
            
            for i in range(0, total_lines, batch_size):
                batch = lines[i:i+batch_size]
                batch_file = os.path.join(temp_streaming_dir, f"batch_{i}.txt")
                
                with open(batch_file, 'w') as f:
                    f.writelines(batch)
                
                print(f"Copied batch {i//batch_size + 1} with {len(batch)} lines to {batch_file}")
                time.sleep(2)  # Wait between batches
                
            print("Log file copying completed successfully")
            
        except Exception as e:
            print(f"Error in copy_log_file thread: {str(e)}")
    
    # Start a thread to copy the log file
    thread = threading.Thread(target=copy_log_file)
    thread.daemon = True
    thread.start()
    
    # Run for 60 seconds then terminate (or until terminated manually)
    print("Streaming query running. Press Ctrl+C to terminate early...")
    query.awaitTermination(60)
    
except Exception as e:
    print(f"Error during query execution: {str(e)}")
    
finally:
    try:
        # Stop the query if it exists and is running
        if 'query' in locals() and query is not None:
            print("Stopping streaming query...")
            query.stop()
            print("Query stopped.")
    except Exception as e:
        print(f"Error stopping query: {str(e)}")
        
    # Clean up temporary directory
    try:
        if os.path.exists(temp_streaming_dir):
            print(f"Removing temporary directory {temp_streaming_dir}...")
            shutil.rmtree(temp_streaming_dir)
            print("Temporary directory removed.")
    except Exception as e:
        print(f"Error cleaning up directory: {str(e)}")
        
    # Cleanly shut down the session
    try:
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped.")
    except Exception as e:
        print(f"Error stopping Spark session: {str(e)}")
    
    print("Application completed.")

