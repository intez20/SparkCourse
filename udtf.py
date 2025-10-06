from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

# ----------------------------
# User-Defined Table Function (UDTF)
# ----------------------------
@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        """Extracts hashtags from the input text."""
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)

# ----------------------------
# User-Defined Function (UDF)
# ----------------------------
@udf(returnType=IntegerType())
def count_hashtags(text: str):
    """Counts the number of hashtags in the input text."""
    if text:
        return len(re.findall(r"#\w+", text))
    return 0

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Python UDTF and UDF Example") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

# Register the UDTF for use in Spark SQL
spark.udtf.register("extract_hashtags", HashtagExtractor)

# Register the UDF for use in Spark SQL
spark.udf.register("count_hashtags", count_hashtags)

# ----------------------------
# Example: Using the UDTF in SQL
# ----------------------------
print("\nUDTF Example (Extract Hashtags):")
spark.sql("SELECT * FROM extract_hashtags('Welcome to #ApacheSpark and #BigData!')").show()
# +------------+
# |    hashtag |
# +------------+
# | #ApacheSpark |
# |   #BigData  |
# +------------+

# ----------------------------
# Example: Using the UDF in SQL
# ----------------------------
print("\nUDF Example (Count Hashtags):")
spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData!') AS hashtag_count").show()
# +--------------+
# |hashtag_count |
# +--------------+
# |      2       |
# +--------------+

# ----------------------------
# Using Both UDTF and UDF with a DataFrame
# ----------------------------
data = [("Learning #AI with #ML",), ("Explore #DataScience",), ("No hashtags here",)]
df = spark.createDataFrame(data, ["text"])

# Apply UDF in a DataFrame query
df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()
# +------------------+-------------+
# |              text| num_hashtags|
# +------------------+-------------+
# |Learning #AI with #ML |     2   |
# | Explore #DataScience |     1   |
# | No hashtags here     |     0   |
# +------------------+-------------+

# Apply UDTF with a LATERAL JOIN
print("\nUsing UDTF with LATERAL JOIN:")
df.createOrReplaceTempView("tweets")
spark.sql(
    "SELECT text, hashtag FROM tweets, LATERAL extract_hashtags(text)"
).show()
# +------------------+-------------+
# |              text|     hashtag |
# +------------------+-------------+
# |Learning #AI with #ML |       #AI |
# |Learning #AI with #ML |       #ML |
# | Explore #DataScience | #DataScience |
# +------------------+-------------+
