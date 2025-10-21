from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lit
import os
import sys
import shutil
import time
import threading

# Create a SparkSession with Windows-friendly settings
spark = SparkSession.builder \
    .appName("SimpleStructuredStreaming") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .master("local[*]") \
    .getOrCreate()

# Reduce logging to ERROR level only
spark.sparkContext.setLogLevel("ERROR")

# Create a clean directory for streaming data
streaming_dir = os.path.join(os.getcwd(), "streaming_input")
if os.path.exists(streaming_dir):
    shutil.rmtree(streaming_dir)
os.makedirs(streaming_dir)
print(f"Created streaming directory: {streaming_dir}")

# Create a simple function to copy a sample log file into the streaming directory
def feed_sample_data():
    time.sleep(5)  # Wait for streaming query to start
    
    # Read our sample log data
    sample_file = os.path.join(os.getcwd(), "sample_logs.txt")
    
    if not os.path.exists(sample_file):
        print(f"ERROR: Sample log file {sample_file} not found!")
        return
        
    print(f"Reading sample data from {sample_file}")
    
    with open(sample_file, 'r') as f:
        lines = f.readlines()
        
    # Feed data in batches
    batch_size = 5
    num_batches = (len(lines) + batch_size - 1) // batch_size
    
    for i in range(num_batches):
        batch = lines[i*batch_size:min((i+1)*batch_size, len(lines))]
        batch_file = os.path.join(streaming_dir, f"batch_{i}.txt")
        
        with open(batch_file, 'w') as f:
            f.writelines(batch)
            
        print(f"Added batch {i+1}/{num_batches} with {len(batch)} lines")
        time.sleep(2)  # Wait between batches

try:
    # Define the schema for the streaming source
    print("Setting up streaming source...")
    
    # Create a streaming DataFrame to read text files
    logs = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load(streaming_dir)
    
    print("Streaming source created successfully")
    
    # Parse Apache access logs
    print("Parsing log format...")
    
    # Extract fields from log lines using regex
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    timestamp_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    method_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    status_pattern = r'\s(\d{3})\s'
    size_pattern = r'\s(\d+)$'
    
    log_df = logs.select(
        regexp_extract('value', host_pattern, 1).alias('host'),
        regexp_extract('value', timestamp_pattern, 1).alias('timestamp'),
        regexp_extract('value', method_pattern, 1).alias('method'),
        regexp_extract('value', method_pattern, 2).alias('endpoint'),
        regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
        regexp_extract('value', size_pattern, 1).cast('integer').alias('size')
    )
    
    # Group by status code and count occurrences
    status_counts = log_df.groupBy('status').count()
    
    # Start the streaming query
    print("Starting streaming query...")
    query = status_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("Query started successfully!")
    
    # Start feeding data in a background thread
    data_thread = threading.Thread(target=feed_sample_data)
    data_thread.daemon = True
    data_thread.start()
    
    # Wait for the query to terminate
    print("Streaming for 30 seconds or until terminated...")
    query.awaitTermination(30)  # Run for 30 seconds

except Exception as e:
    print(f"Error during execution: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    # Clean up
    print("Cleaning up resources...")
    if 'query' in locals() and query is not None:
        query.stop()
        print("Query stopped")
    
    if os.path.exists(streaming_dir):
        shutil.rmtree(streaming_dir)
        print(f"Removed {streaming_dir}")
    
    spark.stop()
    print("Spark session stopped")
    
    print("Application completed")