import os
import time
import shutil
import random
from datetime import datetime

"""
This script continuously feeds log data into a directory
for testing Spark structured streaming applications.
"""

# Source log file
SOURCE_LOG = os.path.join(os.getcwd(), "logs", "access_log.txt")

# Target directory for streaming
TARGET_DIR = os.path.join(os.getcwd(), "streaming_logs")

# Create target directory if it doesn't exist
if not os.path.exists(TARGET_DIR):
    os.makedirs(TARGET_DIR)
    print(f"Created directory {TARGET_DIR}")

print(f"Starting to feed log data from {SOURCE_LOG} to {TARGET_DIR}")
print("Press Ctrl+C to stop the process")

# Read the source log file
with open(SOURCE_LOG, 'r') as f:
    log_lines = f.readlines()

try:
    line_index = 0
    batch_count = 1
    
    while True:
        # Create a batch of 10-50 lines
        batch_size = random.randint(10, 50)
        batch = log_lines[line_index:line_index + batch_size]
        line_index = (line_index + batch_size) % len(log_lines)  # Wrap around to the beginning if needed
        
        # Create a timestamped file name
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        output_file = os.path.join(TARGET_DIR, f"batch_{timestamp}.txt")
        
        # Write the batch to the file
        with open(output_file, 'w') as f:
            f.writelines(batch)
        
        print(f"Batch {batch_count}: Wrote {len(batch)} lines to {output_file}")
        batch_count += 1
        
        # Sleep for a random time between 1-3 seconds
        time.sleep(random.uniform(1, 3))

except KeyboardInterrupt:
    print("\nStopping log data feed")
    print("You can manually clean up the directory if needed")