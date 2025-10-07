from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("HighestRatedMovies").getOrCreate()

df_struct = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

spark_df = spark.read.option("sep", "\t").schema(df_struct).csv("file:///SparkCourse/ml-100k/u.data")

movie_ratings=spark_df.groupBy("movieId").count().orderBy(func.desc("count"))

movie_ratings.show(20)

spark.stop()