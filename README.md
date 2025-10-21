# Apache Spark Course Repository

This repository contains code examples, exercises, and projects from a comprehensive Apache Spark course. It demonstrates the evolution of learning Spark, from basic RDD operations to advanced topics like Spark ML, Structured Streaming, and DataFrames.

## Repository Structure

The repository is organized chronologically based on learning progression, covering the following key areas:

1. **Basic Spark RDD Operations**
2. **Spark SQL and DataFrames**
3. **Data Analysis and Visualization**
4. **Graph Processing**
5. **Recommendation Systems**
6. **Machine Learning with Spark ML**
7. **Streaming with Spark Structured Streaming**

## Learning Journey

### 1. Basic Spark RDD Operations (Late September 2025)

The journey starts with fundamental Spark RDD (Resilient Distributed Dataset) operations:

- **Word Count Examples**:
  - `word-count.py` - Basic word counting
  - `word-count-better.py` - Improved word counting with better data cleaning
  - `word-count-better-sorted.py` - Word counting with sorted results

- **Weather Data Analysis**:
  - `min-temperatures.py` - Finding minimum temperatures by location
  - `max-temperatures.py` - Finding maximum temperatures by location

- **Friends Data Analysis**:
  - `friends-by-age.py` - Calculating average friends by age using RDDs
  - `ratings-counter.py` - Analyzing movie ratings distribution

- **Customer Spending Analysis**:
  - `total-spent-by-customer.py` - Calculating total amount spent by each customer

**Key RDD operations learned:**
- `map` and `mapValues` for transformations
- `reduceByKey` for aggregations
- `filter` for data filtering
- `collect` for retrieving results
- `sortBy` and `sortByKey` for result ordering

### 2. Spark SQL and DataFrames (Early October 2025)

The next phase transitioned to Spark SQL and DataFrames, providing a more structured and optimized approach:

- **DataFrame Conversions**:
  - `friends-by-age-dataframe.py` - Reimplementing the friends analysis using DataFrames
  - `spark-sql-dataframe.py` - Basic Spark SQL operations with DataFrames
  - `min-temperatures-dataframe.py` - Converting the temperature analysis to DataFrame API

- **Advanced DataFrame Operations**:
  - `word-count-better-sorted-dataframe.py` - Word count using DataFrames
  - `total-spent-by-customer-sorted-dataframe.py` - Customer spending analysis using DataFrames

**Key DataFrame concepts learned:**
- DataFrame creation from CSV files
- Schema inference and explicit schema definition using `StructType`
- Column operations and expressions
- Aggregation functions like `avg`, `sum`, `count`
- GroupBy operations
- DataFrame sorting and ordering
- SQL-like querying on DataFrames

### 3. Pandas Integration and UDFs (October 6, 2025)

Next, the course explored the integration between PySpark and pandas, along with User-Defined Functions:

- **Pandas Integration**:
  - `pandas-conversion.py` - Converting between Spark DataFrames and pandas DataFrames
  - `pandas-api.py` - Using pandas API on Spark
  - `pandas-transform-apply.py` - Using pandas transform and apply functions with Spark

- **UDF and UDTF Examples**:
  - `udtf.py` - Using User-Defined Table Functions with Spark SQL

**Key concepts learned:**
- Converting between Spark DataFrames and pandas DataFrames
- Pandas API on Spark
- Creating and using User-Defined Functions (UDFs)
- Using User-Defined Table Functions (UDTFs)
- Performance considerations when using UDFs

### 4. Data Analysis and Visualization (October 6-7, 2025)

The course then moved to more complex data analysis tasks:

- **Log Analysis**:
  - `ApacheAnalyze.ipynb` - Jupyter notebook analyzing Apache access logs
  - `access_log.txt` - Sample Apache log data

- **Movie Ratings Analysis**:
  - `popular-movies-dataframe.py` - Analyzing popular movies
  - `popular-movies-nice-dataframe.py` - Enhanced movie analysis with broadcast variables

**Key concepts learned:**
- Using Jupyter notebooks with Spark
- Broadcast variables for efficient data sharing
- Regular expressions for log parsing
- Complex aggregations and window functions

### 5. Graph Processing (October 7-8, 2025)

The repository includes several examples of graph processing with Spark:

- **Marvel Social Graph Analysis**:
  - `most-popular-superhero-dataframe.py` - Finding the most popular superhero
  - `most-obscure-superheroes.py` - Finding the least connected superheroes
  - `degrees-of-separation.py` - Implementing BFS to find degrees of separation between characters

**Key concepts learned:**
- Graph representation in Spark
- Breadth-First Search (BFS) implementation
- Accumulators for global counters
- Complex join operations
- Processing connected components

### 6. Recommendation Systems (October 8, 2025)

A significant portion of the repository is dedicated to building movie recommendation systems:

- **Base Similarity Calculations**:
  - `movie-similarities-dataframe.py` - Basic movie similarity using cosine similarity

- **Enhanced Recommendation Systems**:
  - `movie-similarities-good-ratings.py` - Filtering for only good ratings
  - `movie-similarities-pearson.py` - Using Pearson correlation coefficient
  - `movie-similarities-jaccard.py` - Using Jaccard similarity
  - `movie-similarities-dynamic-thresholds.py` - Dynamic thresholds based on popularity
  - `movie-similarities-weighted.py` - Weighted similarity metrics
  - `movie-similarities-genre.py` - Using genre information to boost recommendations

**Key concepts learned:**
- Different similarity metrics (Cosine, Pearson, Jaccard)
- Self-joins for item-item collaborative filtering
- Caching and persistence strategies for performance
- Custom weighting and boosting techniques
- Processing and utilizing metadata (genres)

### 7. Machine Learning with Spark ML (October 17, 2025)

The course then explored Spark's machine learning capabilities:

- **Regression Examples**:
  - `spark-linear-regression.py` - Linear regression example
  - `real-estate.py` - Predicting real estate prices

- **Advanced Recommendations**:
  - `movie-recommendations-als-dataframe.py` - Using ALS (Alternating Least Squares) for recommendations
  - `MovieSimilarities1M.py` - Scaling recommendations to larger datasets

**Key concepts learned:**
- Spark ML Pipeline construction
- Feature engineering (vectorization, normalization)
- Model training and evaluation
- Hyperparameter tuning
- Matrix factorization with ALS
- Scaling ML workflows

### 8. Structured Streaming (October 21, 2025)

The most recent additions to the repository focus on Spark's Structured Streaming capabilities:

- **Basic Streaming Examples**:
  - `structured-streaming.py` - Basic structured streaming example
  - `simple_structured_streaming.py` - Simplified streaming for Windows compatibility
  - `log-stream-simulator.py` - Simulating streaming log data

- **Advanced Streaming**:
  - `stream-join-watermarks.py` - Stream-to-stream joins with watermarks
  - `top-urls.py` - Finding top URLs in streaming data
  - `StreamingSQLUDF.py` - Using SQL and UDFs with streaming

- **Helper Utilities**:
  - `feed_log_data.py` - Utility to feed data into streaming applications

**Key concepts learned:**
- Setting up streaming sources and sinks
- Handling streaming aggregations
- Stream-to-stream joins
- Watermarks for late data handling
- Windowed operations
- Stateful processing
- Handling streaming data on Windows systems

## Datasets

The repository includes several datasets for the examples:

- `fakefriends.csv` and `fakefriends-header.csv` - Social network data
- `1800.csv` - Historical weather data
- `customer-orders.csv` - Customer purchase data
- `book.txt` - Text data for word counting examples
- `Marvel-graph.txt` and `Marvel-names.txt` - Marvel character co-appearance data
- `ml-100k/` - MovieLens dataset with 100k ratings
- `access_log.txt` - Apache web server logs
- `realestate.csv` - Real estate pricing data
- `bluesky.jsonl` - Social media data

## Windows Compatibility Notes

Several scripts have been specially adapted for Windows compatibility, particularly the streaming examples that had issues with Hadoop's native IO libraries:

- `simple_structured_streaming.py` - Windows-friendly streaming
- `log-stream-simulator.py` - Adapted for Windows with memory-based log loading

## Running the Examples

Most examples can be run using `spark-submit`:

```
spark-submit example_script.py
```

For Jupyter notebooks:

```
jupyter notebook ApacheAnalyze.ipynb
```

## Learning Progression

This repository shows a clear learning progression:

1. Started with basic RDD operations for simple data processing
2. Moved to DataFrames for more structured and optimized data handling
3. Integrated with pandas for enhanced data manipulation
4. Applied knowledge to complex problems like social graph analysis
5. Built sophisticated recommendation systems with multiple similarity metrics
6. Explored machine learning capabilities
7. Culminated with real-time data processing using structured streaming

## Technologies Used

- Apache Spark (Core, SQL, ML, Streaming)
- PySpark
- pandas
- Jupyter Notebooks
- Python