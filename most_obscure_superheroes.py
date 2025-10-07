from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel-names.txt")

lines = spark.read.text("Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minPopularCount = connections.agg(func.min("connections")).first()[0]

minPopular = connections.filter(func.col("connections") == minPopularCount)

minPopularNames = minPopular.join(names, "id")

print("Least popular superhero(es):"+str(minPopularCount)+" connections")

# for row in minPopularNames:
#     print(f"{row['name']} is the least popular superhero with {row['connections']} co-appearances.")

minPopularNames.select("name").show()

spark.stop()